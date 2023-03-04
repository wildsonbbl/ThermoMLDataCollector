# Inspired by Thermopyl at: https://github.com/choderalab/thermopyl

import numpy as np
import copy
# Obtained by `wget http://media.iupac.org/namespaces/ThermoML/ThermoML.xsd` and `pyxbgen ThermoML.xsd`
from . import thermoml_schema


class Parser(object):
    def __init__(self, filename, Property):
        """Create a parser object from an XML filename."""
        self.filename = filename
        self.root = thermoml_schema.CreateFromDocument(
            open(self.filename).read())
        self.PropertiesOfInterest = ['Pressure, kPa', 'Temperature, K'] + Property
        self.Property = Property
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
        PropertiesOfInterest = self.PropertiesOfInterest
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

            decide = True
            for key in property_dict:
                ePropName = property_dict[key]
                if ePropName in self.Property:
                    decide = False
            if decide:
                continue

            state = dict(filename=self.filename, nDATA=nDATA)

            for key in components:
                state[key] = components[key]

            for key in numtophase:
                state[key] = numtophase[key]

            # This is the only pressure unit used in ThermoML
            state["Pressure, kPa"] = None
            # This is the only temperature unit used in ThermoML
            state['Temperature, K'] = None

            for Constraint in PureOrMixtureData.Constraint:
                nConstraintValue = Constraint.nConstraintValue
                ConstraintType = Constraint.ConstraintID.ConstraintType

                assert len(ConstraintType.orderedContent()) == 1
                constraint_type = ConstraintType.orderedContent()[0].value
                if constraint_type in PropertiesOfInterest:
                    if constraint_type in self.Property:
                        nOrgNum = Constraint.ConstraintID.RegNum.nOrgNum
                        sCommonName = self.compound_num_to_name[nOrgNum]
                        eConstraintPhase = Constraint.ConstraintPhaseID.eConstraintPhase
                        phasenum = phasetophasenum[eConstraintPhase]
                        cn = sCommonNametoCn[sCommonName]
                        coluna = "{} {} {}".format(
                            constraint_type, cn, phasenum)
                        state[coluna] = nConstraintValue
                    else:
                        state[constraint_type] = nConstraintValue

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
                    if vtype in PropertiesOfInterest:
                        if vtype in self.Property:
                            nOrgNum = nOrgNumfromnVarNumber_dict[nVarNumber]
                            sCommonName = self.compound_num_to_name[nOrgNum]
                            cn = sCommonNametoCn[sCommonName]
                            phasenum = eVarPhase_dict[nVarNumber]
                            coluna = "{} {} {}".format(vtype, cn, phasenum)
                            current_data[coluna] = nVarValue
                        else:
                            current_data[vtype] = nVarValue

                for PropertyValue in NumValues.PropertyValue:
                    nPropNumber = PropertyValue.nPropNumber
                    nPropValue = PropertyValue.nPropValue
                    ptype = property_dict[nPropNumber]
                    if ptype in PropertiesOfInterest:
                        phase = ePropPhase_dict[nPropNumber]
                        if ptype in self.Property:
                            nOrgNum = nOrgNumfromnPropNumber_dict[nPropNumber]
                            sCommonName = self.compound_num_to_name[nOrgNum]
                            phasenum = phasetophasenum[phase]
                            cn = sCommonNametoCn[sCommonName]
                            coluna = "{} {} {}".format(ptype, cn, phasenum)
                            current_data[coluna] = nPropValue

                        else:
                            current_data[ptype] = nPropValue

                        """ # Now attempt to extract measurement uncertainty for the same measurement
                        try:
                            uncertainty = PropertyValue.PropUncertainty[0].nStdUncertValue
                        except IndexError:
                            uncertainty = np.nan
                        current_data[ptype + "_std"] = uncertainty"""

                alldata.append(current_data)

        return alldata


def build_pandas_dataframe(filenames, Property):
    """
    Build pandas dataframe for property data and compounds.

    Parameters
    ----------
    filenames : list
        List of ThermoML filenames to process.

    Returns
    -------
    data : pandas DataFrame
        Compiled ThermoML dataframe
    compounds : pandas Dataframe
        Compounds dataframe

    """
    import pandas as pd

    data = []
    compound_dict = {}
    with open('errorLOG.txt', 'w') as f:
        f.write('New run \n')

    for filename in filenames:
        try:
            parser = Parser(filename, Property)
            current_data = parser.parse()
            current_data = pd.DataFrame(current_data)
            data.append(current_data)
            compound_dict.update(parser.compound_name_to_sStandardInChI)
        except Exception as e:
            with open('errorLOG.txt', 'a') as f:
                errormessage = str(e) + '\n error at: %s \n' % filename
                f.write(errormessage)

    # Because the input data is a list of DataFrames, this saves a LOT of memory!  Ignore the index to return unique index.
    data = pd.concat(data, copy=False, ignore_index=True)
    compounds = pd.Series(compound_dict)
    return [data, compounds]
