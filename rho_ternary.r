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

## analysis

tmlframe %>% nrow()
tmlframe %>%
  summary()

### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3, m0_phase) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe %>%
  group_by(m0_phase) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlframe %>%
  filter(
    m0_phase == "Liquid",
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
  ) %>%
  mutate(
    mole_fraction_c1 = if_else(
      mole_fraction_c1 < 0.0,
      0,
      mole_fraction_c1
    ),
    mole_fraction_c2 = if_else(
      mole_fraction_c2 < 0.0,
      0,
      mole_fraction_c2
    ),
    mole_fraction_c3 = if_else(
      mole_fraction_c3 < 0.0,
      0,
      mole_fraction_c3
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
  )

tmlframe %>%
  filter(
    !is.na(mass_fraction_c1),
    !is.na(mass_fraction_c2),
    !is.na(mass_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

# filter concentrations

tmlframe <- tmlframe %>%
  mutate(
    molality_c1 = if_else(
      !is.na(`Molality, mol/kg c2 phase_1`) &
        !is.na(`Molality, mol/kg c3 phase_1`),
      -1,
      `Molality, mol/kg c1 phase_1`
    ),
    molality_c2 = if_else(
      !is.na(`Molality, mol/kg c1 phase_1`) &
        !is.na(`Molality, mol/kg c3 phase_1`),
      -1,
      `Molality, mol/kg c2 phase_1`
    ),
    molality_c3 = if_else(
      !is.na(`Molality, mol/kg c1 phase_1`) &
        !is.na(`Molality, mol/kg c2 phase_1`),
      -1,
      `Molality, mol/kg c3 phase_1`
    ),
  )

tmlframe %>%
  filter(
    !is.na(molality_c1),
    !is.na(molality_c2),
    !is.na(molality_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  view()

tmlframe %>%
  filter(
    (
      !is.na(mole_fraction_c1) &
        !is.na(mole_fraction_c2) &
        !is.na(mole_fraction_c3)
    ) |
      (
        !is.na(mass_fraction_c1) &
          !is.na(mass_fraction_c2) &
          !is.na(mass_fraction_c3)
      ) |
      (
        !is.na(molality_c1) &
          !is.na(molality_c2) &
          !is.na(molality_c3)
      )
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  view()


tmlframe <- tmlframe %>%
  filter(
    (
      !is.na(mole_fraction_c1) &
        !is.na(mole_fraction_c2) &
        !is.na(mole_fraction_c3)
    ) |
      (
        !is.na(mass_fraction_c1) &
          !is.na(mass_fraction_c2) &
          !is.na(mass_fraction_c3)
      ) |
      (
        !is.na(molality_c1) &
          !is.na(molality_c2) &
          !is.na(molality_c3)
      )
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

tmlframe %>%
  summary()

### checking molecules available

tmlframe %>%
  filter(
    `Temperature, K` > 600
  ) %>%
  view()

## Save

tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "rho_ternary.parquet"
  )

tml_saved <- read_parquet("rho_ternary.parquet")
tml_saved %>% colnames()
