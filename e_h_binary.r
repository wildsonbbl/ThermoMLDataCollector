library(tidyverse)
library(arrow)
library(data.table)

## loading file

files <- list.files("./data/raw",
  "dataset.parquet",
  full.names = TRUE
) %>% sort()
files

tmlset <- open_dataset(files, unify_schemas = TRUE)

tmlset %>%
  colnames() %>%
  sort()

## checking type of data

tmlset %>%
  select(type) %>%
  as.data.frame() %>%
  distinct() %>%
  arrange(type)

## selecting properties of interest

tmlset %>%
  filter(
    type == "Excess molar enthalpy (molar enthalpy of mixing), kJ/mol",
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Excess molar enthalpy (molar enthalpy of mixing), kJ/mol",
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlframe %>%
  filter(
    phase_1 != "Crystal" & (phase_1 != "Gas" | !is.na(phase_2))
  ) %>%
  select(where(~ !all(is.na(.x))))

### Fill in missing mass fraction info

tmlframe %>% summary()

tmlframe %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()


tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1 = case_when(
      !is.na(`Molality, mol/kg c1 phase_1`) ~ `Molality, mol/kg c1 phase_1` * molweight1 / 1000,
      !is.na(`Molality, mol/kg c2 phase_1`) ~ 1 - `Molality, mol/kg c2 phase_1` * molweight2 / 1000,
    ),
    mass_fraction_c2 = case_when(
      !is.na(`Molality, mol/kg c2 phase_1`) ~ `Molality, mol/kg c2 phase_1` * molweight2 / 1000,
      !is.na(`Molality, mol/kg c1 phase_1`) ~ 1 - `Molality, mol/kg c1 phase_1` * molweight1 / 1000,
    ),
  )

### Fill in missing mole fraction info

tmlframe %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1 = case_when(
      !is.na(`Mole fraction c1 phase_1`) ~ `Mole fraction c1 phase_1`,
      !is.na(`Mole fraction c1 phase_2`) ~ `Mole fraction c1 phase_2`,
      !is.na(`Mole fraction c2 phase_1`) ~ 1 - `Mole fraction c2 phase_1`,
      !is.na(`Mole fraction c2 phase_2`) ~ 1 - `Mole fraction c2 phase_2`,
      !is.na(`Amount ratio of solute to solvent c2 phase_1`) ~
        1 / (1 + `Amount ratio of solute to solvent c2 phase_1`),
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) ~ (
        (mass_fraction_c1 / molweight1) /
          (mass_fraction_c1 / molweight1 + mass_fraction_c2 / molweight2)
      ),
    ),
    mole_fraction_c2 = case_when(
      !is.na(`Mole fraction c1 phase_1`) ~ 1 - `Mole fraction c1 phase_1`,
      !is.na(`Mole fraction c1 phase_2`) ~ 1 - `Mole fraction c1 phase_2`,
      !is.na(`Mole fraction c2 phase_1`) ~ `Mole fraction c2 phase_1`,
      !is.na(`Mole fraction c2 phase_2`) ~ `Mole fraction c2 phase_2`,
      !is.na(`Amount ratio of solute to solvent c2 phase_1`) ~
        `Amount ratio of solute to solvent c2 phase_1` /
          (1 + `Amount ratio of solute to solvent c2 phase_1`),
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) ~ (
        (mass_fraction_c2 / molweight2) /
          (mass_fraction_c1 / molweight1 + mass_fraction_c2 / molweight2)
      ),
    ),
  )

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1), !is.na(mole_fraction_c2)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()


tmlframe %>%
  mutate(
    T_K = case_when(
      !is.na(`Temperature, K phase_1`) ~ `Temperature, K phase_1`,
      !is.na(`Temperature, K phase_2`) ~ `Temperature, K phase_2`,
    ),
    e_h = case_when(
      !is.na(m0_phase_2) ~ m0_phase_2,
      !is.na(m0_phase_1) ~ m0_phase_1
    ),
    P_kPa = `Pressure, kPa phase_1`
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "e_h_binary.parquet"
  )
