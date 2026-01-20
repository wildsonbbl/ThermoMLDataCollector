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
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlset %>%
  filter(
    type == "Excess molar enthalpy (molar enthalpy of mixing), kJ/mol",
    !is.na(c2),
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()

tmlframe <- tmlset %>%
  filter(
    type == "Excess molar enthalpy (molar enthalpy of mixing), kJ/mol",
    !is.na(c2),
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking and filtering phases

tmlframe %>%
  group_by(phase_1) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlframe %>%
  filter(
    phase_1 != "Crystal"
  ) %>%
  select(where(~ !all(is.na(.x))))

### Fill in missing mass fraction info

tmlframe %>% summary()

tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()

tmlframe %>%
  group_by(`Solvent for m0_phase_1`) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe %>%
  filter(!is.na(
    `Molality, mol/kg c1 phase_1`
  )) %>%
  group_by(
    `Solvent for m0_phase_1`
  ) %>%
  summarise(n = n())

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1 = case_when(
      !is.na(`Molality, mol/kg c1 phase_1`) ~
        `Molality, mol/kg c1 phase_1` * molweight1 / 1000 / (
          1 + `Molality, mol/kg c1 phase_1` * molweight1 / 1000
        ),
    ),
    mass_fraction_c2 = case_when(
      !is.na(`Molality, mol/kg c2 phase_1`) ~
        `Molality, mol/kg c2 phase_1` * molweight2 / 1000 / (
          1 + `Molality, mol/kg c2 phase_1` * molweight2 / 1000
        ),
    ),
    mass_fraction_c3 = case_when(
      !is.na(`Mass fraction c3 phase_1`) ~ `Mass fraction c3 phase_1`,
    )
  )

### Fill in missing mole fraction info

tmlframe %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1 = case_when(
      !is.na(`Mole fraction c1 phase_1`) ~ `Mole fraction c1 phase_1`,
      !is.na(`Mole fraction c2 phase_1`) & !is.na(`Mole fraction c3 phase_1`) ~ 1 - `Mole fraction c2 phase_1` - `Mole fraction c3 phase_1`,
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) & !is.na(mass_fraction_c3) ~
        mass_fraction_c1 / molweight1 / (
          mass_fraction_c1 / molweight1 + mass_fraction_c2 * molweight2 + mass_fraction_c3 * molweight3
        ),
      !is.na(`Solvent: Mole fraction c1 phase_1`) & !is.na(`Mole fraction c2 phase_1`) ~
        (1 - `Mole fraction c2 phase_1`) * `Solvent: Mole fraction c1 phase_1`,
      !is.na(`Solvent: Mole fraction c1 phase_1`) & !is.na(`Mole fraction c3 phase_1`) ~
        (1 - `Mole fraction c3 phase_1`) * `Solvent: Mole fraction c1 phase_1`,
      !is.na(`Solvent: Mole fraction c3 phase_1`) & !is.na(`Mole fraction c2 phase_1`) ~
        (1 - `Mole fraction c2 phase_1`) * (1 - `Solvent: Mole fraction c3 phase_1`),
      !is.na(`Solvent: Mole fraction c2 phase_1`) & !is.na(`Mole fraction c3 phase_1`) ~
        (1 - `Mole fraction c3 phase_1`) * (1 - `Solvent: Mole fraction c2 phase_1`),
    ),
    mole_fraction_c2 = case_when(
      !is.na(`Mole fraction c2 phase_1`) ~ `Mole fraction c2 phase_1`,
      !is.na(`Mole fraction c1 phase_1`) & !is.na(`Mole fraction c3 phase_1`) ~ 1 - `Mole fraction c1 phase_1` - `Mole fraction c3 phase_1`,
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) & !is.na(mass_fraction_c3) ~
        mass_fraction_c2 / molweight2 / (
          mass_fraction_c1 / molweight1 + mass_fraction_c2 * molweight2 + mass_fraction_c3 * molweight3
        ),
      !is.na(`Solvent: Mole fraction c2 phase_1`) & !is.na(`Mole fraction c1 phase_1`) ~
        (1 - `Mole fraction c1 phase_1`) * `Solvent: Mole fraction c2 phase_1`,
      !is.na(`Solvent: Mole fraction c2 phase_1`) & !is.na(`Mole fraction c3 phase_1`) ~
        (1 - `Mole fraction c3 phase_1`) * `Solvent: Mole fraction c2 phase_1`,
      !is.na(`Solvent: Mole fraction c1 phase_1`) & !is.na(`Mole fraction c3 phase_1`) ~
        (1 - `Mole fraction c3 phase_1`) * (1 - `Solvent: Mole fraction c1 phase_1`),
      !is.na(`Solvent: Mole fraction c3 phase_1`) & !is.na(`Mole fraction c1 phase_1`) ~
        (1 - `Mole fraction c1 phase_1`) * (1 - `Solvent: Mole fraction c3 phase_1`),
    ),
    mole_fraction_c3 = case_when(
      !is.na(`Mole fraction c3 phase_1`) ~ `Mole fraction c3 phase_1`,
      !is.na(`Mole fraction c2 phase_1`) & !is.na(`Mole fraction c1 phase_1`) ~ 1 - `Mole fraction c2 phase_1` - `Mole fraction c1 phase_1`,
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) & !is.na(mass_fraction_c3) ~
        mass_fraction_c3 / molweight3 / (
          mass_fraction_c1 / molweight1 + mass_fraction_c2 * molweight2 + mass_fraction_c3 * molweight3
        ),
      !is.na(`Solvent: Mole fraction c3 phase_1`) & !is.na(`Mole fraction c1 phase_1`) ~
        (1 - `Mole fraction c1 phase_1`) * `Solvent: Mole fraction c3 phase_1`,
      !is.na(`Solvent: Mole fraction c3 phase_1`) & !is.na(`Mole fraction c2 phase_1`) ~
        (1 - `Mole fraction c2 phase_1`) * `Solvent: Mole fraction c3 phase_1`,
      !is.na(`Solvent: Mole fraction c1 phase_1`) & !is.na(`Mole fraction c2 phase_1`) ~
        (1 - `Mole fraction c2 phase_1`) * (1 - `Solvent: Mole fraction c1 phase_1`),
      !is.na(`Solvent: Mole fraction c2 phase_1`) & !is.na(`Mole fraction c1 phase_1`) ~
        (1 - `Mole fraction c1 phase_1`) * (1 - `Solvent: Mole fraction c2 phase_1`)
    ),
  )

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1), !is.na(mole_fraction_c2), !is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tmlframe %>%
  filter(
    is.na(mole_fraction_c1) | is.na(mole_fraction_c2) | is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()


tmlframe <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1), !is.na(mole_fraction_c2), !is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x))))

tmlframe %>%
  mutate(
    T_K = `Temperature, K phase_1`,
    e_h = m0_phase_1,
    P_kPa = `Pressure, kPa phase_1`
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "e_h_ternary.parquet"
  )


tml_saved <- read_parquet("e_h_ternary.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()
