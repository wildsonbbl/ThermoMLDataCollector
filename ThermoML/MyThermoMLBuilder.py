# Inspired by Thermopyl at: https://github.com/choderalab/thermopyl

import polars as pl
import copy
# Obtained by `wget http://media.iupac.org/namespaces/ThermoML/ThermoML.xsd` and `pyxbgen ThermoML.xsd`
from . import thermoml_schema


class Parser(object):
    def __init__(self, filename):
        """Create a parser object from an XML filename."""
        self.filename = filename
        self.root = thermoml_schema.CreateFromDocument(
            open(self.filename).read())
        self.store_compounds()

    def store_compounds(self):
        """Extract and store compounds from a thermoml XML file."""
        self.compound_num_to_name = {}
        self.compound_name_to_sStandardInChIKey = {}
        self.compound_name_to_sStandardInChI = {}
        for Compound in self.root.Compound:
            nOrgNum = Compound.RegNum.nOrgNum
            sCommonName = Compound.sCommonName[0]
            sStandardInChIKey = Compound.sStandardInChIKey
            sStandardInChI = Compound.sStandardInChI

            self.compound_num_to_name[nOrgNum] = sCommonName
            self.compound_name_to_sStandardInChIKey[sCommonName] = sStandardInChIKey
            self.compound_name_to_sStandardInChI[sCommonName] = sStandardInChI

    def parse(self):
        """Parse the current XML filename and return a list of measurements."""
        alldata = []
        schema = {}
        for PureOrMixtureData in self.root.PureOrMixtureData:
            current_schema = {}
            nDATA = PureOrMixtureData.nPureOrMixtureDataNumber

            components = {}
            sCommonNametoCn = {}
            sCommonName_list = []
            for Component in PureOrMixtureData.Component:

                nOrgNum = Component.RegNum.nOrgNum
                sCommonName = self.compound_num_to_name[nOrgNum]
                sCommonName_list.append(sCommonName)

            sCommonName_list = sorted(sCommonName_list)
            for (n, name) in enumerate(sCommonName_list):
                n = n+1
                components['c%s' % n] = name
                sCommonNametoCn[name] = 'c%s' % n

            phasetophasenum = {}
            numtophase = {}
            phases_list = []
            for PhaseId in PureOrMixtureData.PhaseID:
                phase = PhaseId.ePhase
                phases_list.append(phase)

            phases_list = sorted(phases_list)
            for (n, phase) in enumerate(phases_list):
                n = n+1
                phasetophasenum[phase] = 'phase_%s' % n
                numtophase['phase_%s' % n] = phase

            property_dict = {}
            ePropPhase_dict = {}
            nOrgNumfromnPropNumber_dict = {}
            for Property in PureOrMixtureData.Property:
                nPropNumber = Property.nPropNumber
                ePropName = Property.Property_MethodID.PropertyGroup.orderedContent()[
                    0].value.ePropName  # ASSUMING LENGTH 1
                property_dict[nPropNumber] = ePropName
                # ASSUMING LENGTH 1
                ePropPhase = Property.PropPhaseID[0].ePropPhase
                ePropPhase_dict[nPropNumber] = ePropPhase
                try:
                    nOrgNum = Property.Property_MethodID.RegNum.nOrgNum
                    nOrgNumfromnPropNumber_dict[nPropNumber] = nOrgNum
                except:
                    continue

            state = dict(filename=self.filename, nDATA=nDATA)
            schema["filename"] = str
            schema['nDATA'] = pl.Int16

            for key in components:
                state[key] = components[key]
                schema[key] = str

            for key in numtophase:
                state[key] = numtophase[key]
                schema[key] = str

            # This is the only pressure unit used in ThermoML
            state["Pressure, kPa"] = None
            # This is the only temperature unit used in ThermoML
            state['Temperature, K'] = None
            schema["Pressure, kPa"] = pl.Float64
            schema["Temperature, K"] = pl.Float64

            for Constraint in PureOrMixtureData.Constraint:
                nConstraintValue = Constraint.nConstraintValue
                ConstraintType = Constraint.ConstraintID.ConstraintType

                assert len(ConstraintType.orderedContent()) == 1
                constraint_type = ConstraintType.orderedContent()[0].value
                try:
                    nOrgNum = Constraint.ConstraintID.RegNum.nOrgNum
                    sCommonName = self.compound_num_to_name[nOrgNum]
                    eConstraintPhase = Constraint.ConstraintPhaseID.eConstraintPhase
                    phasenum = phasetophasenum[eConstraintPhase]
                    cn = sCommonNametoCn[sCommonName]
                    coluna = "{} {} {}".format(
                        constraint_type, cn, phasenum)
                    state[coluna] = nConstraintValue
                    schema[coluna] = pl.Float64
                except:
                    state[constraint_type] = nConstraintValue
                    schema[constraint_type] = pl.Float64

            variable_dict = {}
            nOrgNumfromnVarNumber_dict = {}
            eVarPhase_dict = {}
            for Variable in PureOrMixtureData.Variable:
                nVarNumber = Variable.nVarNumber
                VariableType = Variable.VariableID.VariableType
                assert len(VariableType.orderedContent()) == 1
                # Assume length 1, haven't found counterexample yet.
                vtype = VariableType.orderedContent()[0].value
                variable_dict[nVarNumber] = vtype

                try:
                    nOrgNum = Variable.VariableID.RegNum.nOrgNum
                    nOrgNumfromnVarNumber_dict[nVarNumber] = nOrgNum
                    eVarPhase = Variable.VarPhaseID.eVarPhase
                    phasenum = phasetophasenum[eVarPhase]
                    eVarPhase_dict[nVarNumber] = phasenum
                except:
                    continue

            for NumValues in PureOrMixtureData.NumValues:
                # Copy in values of constraints.
                current_data = copy.deepcopy(state)
                for VariableValue in NumValues.VariableValue:
                    nVarValue = VariableValue.nVarValue
                    nVarNumber = VariableValue.nVarNumber
                    vtype = variable_dict[nVarNumber]
                    try:
                        nOrgNum = nOrgNumfromnVarNumber_dict[nVarNumber]
                        sCommonName = self.compound_num_to_name[nOrgNum]
                        cn = sCommonNametoCn[sCommonName]
                        phasenum = eVarPhase_dict[nVarNumber]
                        coluna = "{} {} {}".format(vtype, cn, phasenum)
                        current_data[coluna] = nVarValue
                        schema[coluna] = pl.Float64
                    except:
                        current_data[vtype] = nVarValue
                        schema[vtype] = pl.Float64

                for PropertyValue in NumValues.PropertyValue:
                    nPropNumber = PropertyValue.nPropNumber
                    nPropValue = PropertyValue.nPropValue
                    ptype = property_dict[nPropNumber]
                    try:
                        nOrgNum = nOrgNumfromnPropNumber_dict[nPropNumber]
                        sCommonName = self.compound_num_to_name[nOrgNum]
                        phase = ePropPhase_dict[nPropNumber]
                        phasenum = phasetophasenum[phase]
                        cn = sCommonNametoCn[sCommonName]
                        coluna = "{} {} {}".format(ptype, cn, phasenum)
                        current_data[coluna] = nPropValue
                        schema[coluna] = pl.Float64
                    except:
                        current_data[ptype] = nPropValue
                        schema[ptype] = pl.Float64

                    """ # Now attempt to extract measurement uncertainty for the same measurement
                        try:
                            uncertainty = PropertyValue.PropUncertainty[0].nStdUncertValue
                        except IndexError:
                            uncertainty = np.nan
                        current_data[ptype + "_std"] = uncertainty"""
                alldata.append(current_data)

        return alldata, schema


def build_dataset(filenames: list) -> list[pl.DataFrame, pl.DataFrame]:
    """
    Build dataset for property data and compounds.

    Parameters
    ----------
    filenames : list
        List of ThermoML filenames to process.

    Returns
    -------
    data : polars DataFrame
        Compiled ThermoML DataFrame
    compounds : polars DataFrame
        Compounds DataFrame

    """

    data = pl.DataFrame()
    compound_dict = {}
    schema_dict = {}
    with open('errorLOG.txt', 'w') as f:
        f.write('New run \n')

    for filename in filenames:
        try:
            parser = Parser(filename)
            current_data, current_schema = parser.parse()
            schema_dict.update(current_schema)
            current_data = pl.DataFrame(current_data, schema_dict)
            data = pl.concat([data, current_data], how='diagonal')
            compound_dict.update(parser.compound_name_to_sStandardInChI)
        except Exception as e:
            with open('errorLOG.txt', 'a') as f:
                errormessage = str(e) + '\n error at: %s \n' % filename
                f.write(errormessage)

    compounds = pl.DataFrame(
        {'CommonName': compound_dict.keys(),
         'StandardInChI': compound_dict.values()}
    )
    return [data, compounds]
