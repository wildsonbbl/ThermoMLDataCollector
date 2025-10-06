"ThermoML XML parser and dataset builder."

# pylint: disable=invalid-name

# Inspired by Thermopyl at: https://github.com/choderalab/thermopyl

import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
from pyxb import ContentValidationError, SimpleFacetValueError
from rdkit import Chem
from rdkit.Chem.rdMolDescriptors import (  # pylint: disable=no-name-in-module
    CalcExactMolWt,
)
from tqdm import tqdm

from . import thermoml_schema


def mol_weight_from_inchi(inchi: str) -> float:
    """Calculate molecular weight from InChI string."""
    if inchi is None:
        return float("nan")
    mol = Chem.MolFromInchi(inchi)
    if mol is None:
        mol = Chem.MolFromInchi(inchi, sanitize=False)
    if mol is None:
        raise ValueError(f"Invalid InChI string: {inchi}")
    return CalcExactMolWt(mol)


class Parser:
    """Parser for ThermoML XML files."""

    def __init__(self, filename: str):
        """Create a parser object from an XML filename."""
        self.filename = filename
        # Uso de context manager para garantir fechamento do arquivo
        with open(self.filename, "r", encoding="utf-8") as fh:
            self.root = thermoml_schema.CreateFromDocument(fh.read())
        self.store_compounds()

    def store_compounds(self):
        """Extract and store compounds from a thermoml XML file."""
        self.compound_num_to_name = {}
        self.compound_name_to_sStandardInChI = {}
        self.compound_name_to_molweight = {}
        for Compound in self.root.Compound:
            nOrgNum = Compound.RegNum.nOrgNum
            sCommonName = Compound.sCommonName[0]
            sStandardInChI = Compound.sStandardInChI

            self.compound_num_to_name[nOrgNum] = sCommonName
            self.compound_name_to_sStandardInChI[sCommonName] = sStandardInChI
            self.compound_name_to_molweight[sCommonName] = mol_weight_from_inchi(
                sStandardInChI
            )

    # pylint: disable=too-many-locals,too-many-statements,too-many-branches
    def parse(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Parse the current XML filename and return a list of measurements."""
        alldata: List[Dict[str, Any]] = []
        schema: Dict[str, Any] = {}
        schema["filename"] = str
        schema["nDATA"] = pl.Int32
        for PureOrMixtureData in self.root.PureOrMixtureData:
            nDATA = PureOrMixtureData.nPureOrMixtureDataNumber

            Cn_to_sCommonName = {}
            sCommonNametoCn = {}
            sCommonName_list = []
            for Component in PureOrMixtureData.Component:
                nOrgNum = Component.RegNum.nOrgNum
                sCommonName = self.compound_num_to_name[nOrgNum]
                sCommonName_list.append(sCommonName)

            sCommonName_list = sorted(sCommonName_list)
            for idx, name in enumerate(sCommonName_list, start=1):
                Cn_to_sCommonName[idx] = name
                sCommonNametoCn[name] = idx

            phase_to_phasenum = {}
            numtophase = {}
            phases_list = sorted(
                [PhaseId.ePhase for PhaseId in PureOrMixtureData.PhaseID]
            )
            for idx, phase in enumerate(phases_list, start=1):
                tag = f"phase_{idx}"
                phase_to_phasenum[phase] = tag
                numtophase[tag] = phase

            nPropNumber_to_ePropName = {}
            nPropNumber_to_phasenum = {}
            nPropNumber_to_nOrgNum = {}
            nPropNumber_to_solvent = {}
            for Property in PureOrMixtureData.Property:
                nPropNumber = Property.nPropNumber
                ordered = Property.Property_MethodID.PropertyGroup.orderedContent()
                if not ordered:
                    continue
                ePropName = ordered[0].value.ePropName
                nPropNumber_to_ePropName[nPropNumber] = ePropName
                if Property.PropPhaseID:
                    ePropPhase = Property.PropPhaseID[0].ePropPhase
                    nPropNumber_to_phasenum[nPropNumber] = phase_to_phasenum.get(
                        ePropPhase
                    )
                try:
                    nOrgNum = Property.Property_MethodID.RegNum.nOrgNum
                    nPropNumber_to_nOrgNum[nPropNumber] = nOrgNum
                except AttributeError:
                    pass
                try:
                    solvents = ""
                    for RegNum in Property.Solvent.RegNum:
                        nOrgNum = RegNum.nOrgNum
                        sCommonName = self.compound_num_to_name[nOrgNum]
                        cn = sCommonNametoCn[sCommonName]
                        solvents += f"c{cn} "
                    nPropNumber_to_solvent[nPropNumber] = solvents.strip()
                except (AttributeError, IndexError):
                    pass

            nVarNumber_to_vtype = {}
            nVarNumber_to_phasenum = {}
            nVarNumber_to_nOrgNum = {}
            nVarNumber_to_solvent = {}
            for Variable in PureOrMixtureData.Variable:
                nVarNumber = Variable.nVarNumber
                ordered = Variable.VariableID.VariableType.orderedContent()
                if not ordered:
                    continue
                vtype = ordered[0].value
                nVarNumber_to_vtype[nVarNumber] = vtype
                eVarPhase = Variable.VarPhaseID.eVarPhase
                nVarNumber_to_phasenum[nVarNumber] = phase_to_phasenum.get(eVarPhase)
                try:
                    nOrgNum = Variable.VariableID.RegNum.nOrgNum
                    nVarNumber_to_nOrgNum[nVarNumber] = nOrgNum
                except AttributeError:
                    pass
                try:
                    solvents = ""
                    for RegNum in Variable.Solvent.RegNum:
                        nOrgNum = RegNum.nOrgNum
                        sCommonName = self.compound_num_to_name[nOrgNum]
                        cn = sCommonNametoCn[sCommonName]
                        solvents += f"c{cn} "
                    nVarNumber_to_solvent[nVarNumber] = solvents.strip()
                except (AttributeError, IndexError):
                    pass

            state = {"filename": self.filename, "nDATA": nDATA}

            for idx, comp_name in Cn_to_sCommonName.items():
                state[f"c{idx}"] = comp_name
                schema[f"c{idx}"] = str
                state[f"inchi{idx}"] = self.compound_name_to_sStandardInChI[comp_name]
                schema[f"inchi{idx}"] = str
                state[f"molweight{idx}"] = self.compound_name_to_molweight[comp_name]
                schema[f"molweight{idx}"] = pl.Float64

            for phase_key, phase_name in numtophase.items():
                state[phase_key] = phase_name
                schema[phase_key] = str

            # Constraints
            for Constraint in PureOrMixtureData.Constraint:
                ConstraintTypeNode = Constraint.ConstraintID.ConstraintType
                if len(ConstraintTypeNode.orderedContent()) != 1:
                    continue
                constraint_type = ConstraintTypeNode.orderedContent()[0].value
                eConstraintPhase = Constraint.ConstraintPhaseID.eConstraintPhase
                phasenum = phase_to_phasenum.get(eConstraintPhase, "phase_?")
                try:
                    nOrgNum = Constraint.ConstraintID.RegNum.nOrgNum
                    sCommonName = self.compound_num_to_name[nOrgNum]
                    cn = sCommonNametoCn[sCommonName]
                    coluna = f"{constraint_type} c{cn} {phasenum}"
                except AttributeError:
                    coluna = f"{constraint_type} {phasenum}"
                state[coluna] = Constraint.nConstraintValue
                schema[coluna] = pl.Float64
                try:
                    solvents = ""
                    for RegNum in Constraint.Solvent.RegNum:
                        nOrgNum = RegNum.nOrgNum
                        sCommonName = self.compound_num_to_name[nOrgNum]
                        cn = sCommonNametoCn[sCommonName]
                        solvents += f"c{cn} "
                    state[f"Solvent for {coluna}"] = solvents.strip()
                    schema[f"Solvent for {coluna}"] = str
                except (AttributeError, IndexError):
                    pass

            for NumValues in PureOrMixtureData.NumValues:
                # Shallow copy suficiente (melhor que deepcopy)
                current_data = state.copy()

                for VariableValue in NumValues.VariableValue:
                    nVarNumber = VariableValue.nVarNumber
                    if nVarNumber not in nVarNumber_to_vtype:
                        continue
                    phasenum = nVarNumber_to_phasenum.get(nVarNumber)
                    vtype = nVarNumber_to_vtype[nVarNumber]
                    try:
                        nOrgNum = nVarNumber_to_nOrgNum[nVarNumber]
                        sCommonName = self.compound_num_to_name[nOrgNum]
                        cn = sCommonNametoCn[sCommonName]
                        coluna = f"{vtype} c{cn} {phasenum}"
                    except KeyError:
                        coluna = f"{vtype} {phasenum}"
                    current_data[coluna] = VariableValue.nVarValue
                    schema[coluna] = pl.Float64
                    try:
                        solvent = nVarNumber_to_solvent[nVarNumber]
                        coluna = f"Solvent for {coluna}"
                        current_data[coluna] = solvent
                        schema[coluna] = str
                    except KeyError:
                        pass

                for PropertyValue in NumValues.PropertyValue:
                    nPropNumber = PropertyValue.nPropNumber
                    if nPropNumber not in nPropNumber_to_ePropName:
                        continue
                    ptype = nPropNumber_to_ePropName[nPropNumber]
                    phasenum = nPropNumber_to_phasenum.get(nPropNumber)
                    current_data["type"] = ptype
                    schema["type"] = str
                    try:
                        nOrgNum = nPropNumber_to_nOrgNum[nPropNumber]
                        sCommonName = self.compound_num_to_name[nOrgNum]
                        cn = sCommonNametoCn[sCommonName]
                        coluna = f"m{cn}_{phasenum}"
                    except KeyError:
                        coluna = f"m0_{phasenum}"
                    current_data[coluna] = PropertyValue.nPropValue
                    schema[coluna] = pl.Float64
                    try:
                        solvent = nPropNumber_to_solvent[nPropNumber]
                        coluna = f"Solvent for {coluna}"
                        current_data[coluna] = solvent
                        schema[coluna] = str
                    except KeyError:
                        pass

                alldata.append(current_data)

        return alldata, schema


# pylint: disable=too-many-locals
def build_dataset(
    filenames: List[str],
    subdir: str,
    executor: str = "process",
    max_workers: int | None = None,
    final_parquet: str = "dataset.parquet",
) -> pl.LazyFrame:
    """
    Build a dataset from ThermoML XML files.

    Parameters
    ----------
    filenames : list[str]
      Paths to ThermoML XML files.
    subdir : str
      Output subdirectory inside data/.
    executor : str
      'process' for CPU-bound parsing or 'thread' for I/O-bound parsing.
    max_workers : int | None
      Number of worker processes/threads (None => library default).
    final_parquet : str
      Name of consolidated output parquet inside data/subdir.

    Returns
    -------
    pl.LazyFrame
      LazyFrame of the consolidated parquet.
    """
    if executor not in {"process", "thread"}:
        raise ValueError("executor deve ser 'process' ou 'thread'")

    savedir = os.path.join("data", subdir)

    error_log = os.path.join(savedir, "errorLOG.txt")
    with open(error_log, "w", encoding="utf-8") as f:
        f.write("New run\n")

    alldata: List[Dict[str, Any]] = []
    all_schema: Dict[str, Any] = {}
    ExecutorCls = ProcessPoolExecutor if executor == "process" else ThreadPoolExecutor

    with ExecutorCls(max_workers=max_workers) as pool:
        futures = {pool.submit(_worker_parse, fn): fn for fn in filenames}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="files"):
            alldata_file, schema, error = fut.result()
            if error:
                with open(error_log, "a", encoding="utf-8") as f:
                    f.write(error)
                continue
            if alldata_file and schema:
                alldata.extend(alldata_file)
                all_schema.update(schema)

    final_path = os.path.join(savedir, final_parquet)
    alldata_df = pl.DataFrame(alldata, all_schema)
    alldata_df.write_parquet(final_path)

    return alldata_df.lazy()


def _worker_parse(
    filename: str,
) -> Tuple[
    Optional[List[Dict[str, Any]]],
    Optional[Dict[str, Any]],
    Optional[str],
]:
    """
    Parse ThermoML XML file.

    Args:
        filename (str): Path to the ThermoML XML file.
    Returns:
        output (Tuple):
            - List of parsed data dictionaries (or None on error).
            - Schema dictionary (or None on error).
            - Error message string (or None if no error).
    """
    try:
        parser = Parser(filename)
        alldata, schema = parser.parse()
        return alldata, schema, None
    except (
        ContentValidationError,
        SimpleFacetValueError,
        UnicodeDecodeError,
        ValueError,
    ) as e:
        err_type = type(e).__name__
        return None, None, f"Error parsing {filename} [{err_type}]: {e}\n"
