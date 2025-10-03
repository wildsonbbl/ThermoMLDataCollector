# pylint: disable=all
# type: ignore
# Inspired by Thermopyl at: https://github.com/choderalab/thermopyl

import os
import shutil
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import List

import polars as pl
from tqdm import tqdm

from . import thermoml_schema


class Parser(object):
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
        for Compound in self.root.Compound:
            nOrgNum = Compound.RegNum.nOrgNum
            sCommonName = Compound.sCommonName[0]
            sStandardInChI = Compound.sStandardInChI

            self.compound_num_to_name[nOrgNum] = sCommonName
            self.compound_name_to_sStandardInChI[sCommonName] = sStandardInChI

    def parse(self):
        """Parse the current XML filename and return a list of measurements."""
        alldata = []
        schema = {}
        for PureOrMixtureData in self.root.PureOrMixtureData:
            nDATA = PureOrMixtureData.nPureOrMixtureDataNumber

            components = {}
            sCommonNametoCn = {}
            sCommonName_list = []
            for Component in PureOrMixtureData.Component:
                nOrgNum = Component.RegNum.nOrgNum
                sCommonName = self.compound_num_to_name[nOrgNum]
                sCommonName_list.append(sCommonName)

            sCommonName_list = sorted(sCommonName_list)
            for idx, name in enumerate(sCommonName_list, start=1):
                components[idx] = name
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

            nVarNumber_to_vtype = {}
            nVarNumber_to_phasenum = {}
            nVarNumber_to_nOrgNum = {}
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

            state = dict(filename=self.filename, nDATA=nDATA)
            schema = {}
            schema["filename"] = str
            schema["nDATA"] = pl.Int32

            for key, comp_name in components.items():
                state[f"c{key}"] = comp_name
                schema[f"c{key}"] = str
                state[f"inchi{key}"] = self.compound_name_to_sStandardInChI[comp_name]
                schema[f"inchi{key}"] = str

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

                alldata.append(current_data)

        return alldata, schema


def build_dataset(
    filenames: List[str],
    subdir: str,
    executor: str = "process",
    max_workers: int | None = None,
    final_parquet: str = "dataset.parquet",
    clean_temps: bool = False,
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
    clean_temps : bool
      Remove temporary parquet files after consolidation.

    Returns
    -------
    pl.LazyFrame
      LazyFrame of the consolidated parquet.
    """
    if executor not in {"process", "thread"}:
        raise ValueError("executor deve ser 'process' ou 'thread'")

    savedir = os.path.join("data", subdir)
    # Limpa o diretório de saída antes de continuar
    if os.path.isdir(savedir):
        for entry in os.scandir(savedir):
            try:
                if entry.is_file() or entry.is_symlink():
                    os.unlink(entry.path)
                elif entry.is_dir():
                    shutil.rmtree(entry.path)
            except Exception:
                pass
    os.makedirs(savedir, exist_ok=True)

    error_log = os.path.join(savedir, "errorLOG.txt")
    with open(error_log, "w", encoding="utf-8") as f:
        f.write("New run\n")

    produced_files: list[str] = []
    ExecutorCls = ProcessPoolExecutor if executor == "process" else ThreadPoolExecutor

    with ExecutorCls(max_workers=max_workers) as pool:
        futures = {pool.submit(_worker_parse, fn, savedir): fn for fn in filenames}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="files"):
            filename, _schema_part, parquet_path, error = fut.result()
            if error:
                with open(error_log, "a", encoding="utf-8") as f:
                    f.write(f"{error}\n error at: {filename}\n")
                continue
            if parquet_path:
                produced_files.append(parquet_path)

    if not produced_files:
        return pl.DataFrame().lazy()

    final_path = os.path.join(savedir, final_parquet)
    lf = pl.concat([pl.scan_parquet(f) for f in produced_files], how="diagonal")

    try:
        lf.sink_parquet(final_path)
    except AttributeError:
        lf.collect(streaming=True).write_parquet(final_path)

    if clean_temps:
        for f in produced_files:
            if os.path.abspath(f) != os.path.abspath(final_path):
                try:
                    os.remove(f)
                except OSError:
                    pass

    return pl.scan_parquet(final_path)


def _worker_parse(filename: str, savedir: str):
    """
    Parse ThermoML XML file.
    """
    try:
        parser = Parser(filename)
        rows, schema_part = parser.parse()
        out_path = os.path.join(savedir, f"tmp_{uuid.uuid4().hex}.parquet")
        if rows:
            df = pl.DataFrame(rows)
            df.write_parquet(out_path)
        else:
            # Evita parquet totalmente vazio sem schema (coloca coluna sentinel)
            pl.DataFrame({"_empty": []}).write_parquet(out_path)
        return filename, schema_part, out_path, None
    except Exception as e:
        return filename, None, None, str(e)
