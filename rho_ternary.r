library(tidyverse)
library(arrow)
library(data.table)

## loading file

files <- list.files("./data/raw",
  "*.parquet",
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
    type == "Mass density, kg/m3",
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Mass density, kg/m3",
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe %>%
  filter(
    phase_1 == "Liquid",
    is.na(phase_2)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlframe %>%
  filter(
    phase_1 == "Liquid",
    is.na(phase_2)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))


### Fill in missing mole fraction info


tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1 = if_else(
      is.na(`Mole fraction c1 phase_1`),
      1 - `Mole fraction c2 phase_1` - `Mole fraction c3 phase_1`,
      `Mole fraction c1 phase_1`
    ),
    mole_fraction_c2 = if_else(
      is.na(`Mole fraction c2 phase_1`),
      1 - `Mole fraction c1 phase_1` - `Mole fraction c3 phase_1`,
      `Mole fraction c2 phase_1`
    ),
    mole_fraction_c3 = if_else(
      is.na(`Mole fraction c3 phase_1`),
      1 - `Mole fraction c1 phase_1` - `Mole fraction c2 phase_1`,
      `Mole fraction c3 phase_1`
    )
  )

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1),
    !is.na(mole_fraction_c2),
    !is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()


### Fill in missing mass fraction info

tmlframe %>%
  filter(
    !is.na(`Solvent: Mass fraction c1 phase_1`)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe %>%
  filter(
    !is.na(`Solvent: Mass fraction c1 phase_1`)
  ) %>%
  group_by(`Solvent for m0_phase_1`) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe %>%
  filter(
    !is.na(`Solvent: Mass fraction c2 phase_1`)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe %>%
  filter(
    !is.na(`Solvent: Mass fraction c2 phase_1`)
  ) %>%
  group_by(`Solvent for m0_phase_1`) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1 = if_else(
      is.na(`Mass fraction c1 phase_1`),
      1 - `Mass fraction c2 phase_1` - `Mass fraction c3 phase_1`,
      `Mass fraction c1 phase_1`
    ),
    mass_fraction_c2 = if_else(
      is.na(`Mass fraction c2 phase_1`),
      1 - `Mass fraction c1 phase_1` - `Mass fraction c3 phase_1`,
      `Mass fraction c2 phase_1`
    ),
    mass_fraction_c3 = if_else(
      is.na(`Mass fraction c3 phase_1`),
      1 - `Mass fraction c1 phase_1` - `Mass fraction c2 phase_1`,
      `Mass fraction c3 phase_1`
    )
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      !is.na(`Molality, mol/kg c1 phase_1`),
      `Molality, mol/kg c1 phase_1` * molweight1 / 1000 / (
        1 + `Molality, mol/kg c1 phase_1` * molweight1 / 1000
      ),
      mass_fraction_c1
    ),
    mass_fraction_c2 = if_else(
      !is.na(`Molality, mol/kg c2 phase_1`),
      `Molality, mol/kg c2 phase_1` * molweight2 / 1000 / (
        1 + `Molality, mol/kg c2 phase_1` * molweight2 / 1000
      ),
      mass_fraction_c2
    ),
    mass_fraction_c3 = if_else(
      !is.na(`Molality, mol/kg c3 phase_1`),
      `Molality, mol/kg c3 phase_1` * molweight3 / 1000 / (
        1 + `Molality, mol/kg c3 phase_1` * molweight3 / 1000
      ),
      mass_fraction_c3
    ),
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      is.na(mass_fraction_c1) &
        !is.na(mass_fraction_c2) &
        !is.na(mass_fraction_c3),
      1 - mass_fraction_c2 - mass_fraction_c3,
      mass_fraction_c1
    ),
    mass_fraction_c2 = if_else(
      !is.na(mass_fraction_c1) &
        is.na(mass_fraction_c2) &
        !is.na(mass_fraction_c3),
      1 - mass_fraction_c1 - mass_fraction_c3,
      mass_fraction_c2
    ),
    mass_fraction_c3 = if_else(
      !is.na(mass_fraction_c1) &
        !is.na(mass_fraction_c2) &
        is.na(mass_fraction_c3),
      1 - mass_fraction_c1 - mass_fraction_c2,
      mass_fraction_c3
    ),
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      !is.na(`Solvent: Mass fraction c1 phase_1`) &
        !is.na(mass_fraction_c2),
      (1 - mass_fraction_c2) * `Solvent: Mass fraction c1 phase_1`,
      mass_fraction_c1
    ),
    mass_fraction_c2 = if_else(
      !is.na(`Solvent: Mass fraction c2 phase_1`) &
        !is.na(mass_fraction_c1),
      (1 - mass_fraction_c1) * `Solvent: Mass fraction c2 phase_1`,
      mass_fraction_c2
    ),
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      !is.na(`Solvent: Mass fraction c1 phase_1`) &
        !is.na(mass_fraction_c3),
      (1 - mass_fraction_c3) * `Solvent: Mass fraction c1 phase_1`,
      mass_fraction_c1
    ),
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      is.na(mass_fraction_c1) &
        !is.na(mass_fraction_c2) &
        !is.na(mass_fraction_c3),
      1 - mass_fraction_c2 - mass_fraction_c3,
      mass_fraction_c1
    ),
    mass_fraction_c2 = if_else(
      !is.na(mass_fraction_c1) &
        is.na(mass_fraction_c2) &
        !is.na(mass_fraction_c3),
      1 - mass_fraction_c1 - mass_fraction_c3,
      mass_fraction_c2
    ),
    mass_fraction_c3 = if_else(
      !is.na(mass_fraction_c1) &
        !is.na(mass_fraction_c2) &
        is.na(mass_fraction_c3),
      1 - mass_fraction_c1 - mass_fraction_c2,
      mass_fraction_c3
    ),
  )

tmlframe %>%
  filter(
    !is.na(mass_fraction_c1) |
      !is.na(mass_fraction_c2) |
      !is.na(mass_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

# fill in missing molality

tmlframe %>%
  filter(
    !is.na(`Solvent: Molality, mol/kg c2 phase_1`)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe %>%
  filter(
    !is.na(`Solvent: Molality, mol/kg c2 phase_1`)
  ) %>%
  group_by(`Solvent for m0_phase_1`) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe %>%
  filter(
    !is.na(`Solvent: Molality, mol/kg c1 phase_1`)
  ) %>%
  group_by(`Solvent for m0_phase_1`) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe %>%
  filter(
    !is.na(`Solvent: Molality, mol/kg c1 phase_1`)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c2 = if_else(
      !is.na(`Solvent: Molality, mol/kg c2 phase_1`) &
        !is.na(mass_fraction_c1),
      `Solvent: Molality, mol/kg c2 phase_1` * molweight2 / 1000 / (
        1 + `Solvent: Molality, mol/kg c2 phase_1` * molweight2 / 1000
      ) * (1 - mass_fraction_c1),
      mass_fraction_c2
    ),
    mass_fraction_c1 = if_else(
      !is.na(`Solvent: Molality, mol/kg c1 phase_1`) &
        !is.na(mass_fraction_c2),
      `Solvent: Molality, mol/kg c1 phase_1` * molweight1 / 1000 / (
        1 + `Solvent: Molality, mol/kg c1 phase_1` * molweight1 / 1000
      ) * (1 - mass_fraction_c2),
      mass_fraction_c1
    ),
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      is.na(mass_fraction_c1) &
        !is.na(mass_fraction_c2) &
        !is.na(mass_fraction_c3),
      1 - mass_fraction_c2 - mass_fraction_c3,
      mass_fraction_c1
    ),
    mass_fraction_c2 = if_else(
      !is.na(mass_fraction_c1) &
        is.na(mass_fraction_c2) &
        !is.na(mass_fraction_c3),
      1 - mass_fraction_c1 - mass_fraction_c3,
      mass_fraction_c2
    ),
    mass_fraction_c3 = if_else(
      !is.na(mass_fraction_c1) &
        !is.na(mass_fraction_c2) &
        is.na(mass_fraction_c3),
      1 - mass_fraction_c1 - mass_fraction_c2,
      mass_fraction_c3
    ),
  ) %>%
  mutate(
    mole_fraction_c1 = if_else(
      !is.na(mass_fraction_c1) &
        !is.na(mass_fraction_c2) &
        !is.na(mass_fraction_c3),
      (
        mass_fraction_c1 /
          molweight1
      ) /
        (
          mass_fraction_c1 / molweight1 +
            mass_fraction_c2 / molweight2 +
            mass_fraction_c3 / molweight3
        ),
      mole_fraction_c1
    ),
    mole_fraction_c2 = if_else(
      !is.na(mass_fraction_c1) &
        !is.na(mass_fraction_c2) &
        !is.na(mass_fraction_c3),
      (
        mass_fraction_c2 /
          molweight2
      ) /
        (
          mass_fraction_c1 / molweight1 +
            mass_fraction_c2 / molweight2 +
            mass_fraction_c3 / molweight3
        ),
      mole_fraction_c2
    ),
    mole_fraction_c3 = if_else(
      !is.na(mass_fraction_c1) &
        !is.na(mass_fraction_c2) &
        !is.na(mass_fraction_c3),
      (
        mass_fraction_c3 /
          molweight3
      ) /
        (
          mass_fraction_c1 / molweight1 +
            mass_fraction_c2 / molweight2 +
            mass_fraction_c3 / molweight3
        ),
      mole_fraction_c3
    ),
  )

tmlframe %>%
  filter(
    is.na(mole_fraction_c1),
    is.na(mole_fraction_c2),
    is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe %>%
  filter(
    mass_fraction_c3 < 0
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

# filter concentrations

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1),
    !is.na(mole_fraction_c2),
    !is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()



tmlframe <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1),
    !is.na(mole_fraction_c2),
    !is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))


### checking molecules available

tmlframe %>%
  filter(
    `Temperature, K phase_1` > 600
  ) %>%
  summary()

## Save

tmlframe %>%
  summary()

tmlframe %>%
  rename(
    T_K = `Temperature, K phase_1`,
    rho = m0_phase_1,
    P_kPa = `Pressure, kPa phase_1`
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "rho_ternary.parquet"
  )

tml_saved <- read_parquet("rho_ternary.parquet")
tml_saved %>% colnames()
