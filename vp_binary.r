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
    type == "Vapor or sublimation pressure, kPa",
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Vapor or sublimation pressure, kPa",
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))


### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3, phase_4) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe %>%
  filter(
    phase_1 == "Gas",
    phase_2 == "Liquid",
    is.na(phase_3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlframe %>%
  filter(
    phase_1 == "Gas",
    phase_2 == "Liquid",
    is.na(phase_3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

tmlframe %>%
  filter(
    m0_phase_2 < 0
  ) %>%
  view()

### Fill in missing mole fraction info

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1 = if_else(
      is.na(`Mole fraction c1 phase_2`),
      1 - `Mole fraction c2 phase_2`,
      `Mole fraction c1 phase_2`
    ),
    mole_fraction_c2 = if_else(
      is.na(`Mole fraction c2 phase_2`),
      1 - `Mole fraction c1 phase_2`,
      `Mole fraction c2 phase_2`
    )
  )

### Fill in missing mass fraction info

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1 = if_else(
      is.na(`Mass fraction c1 phase_2`),
      1 - `Mass fraction c2 phase_2`,
      `Mass fraction c1 phase_2`
    ),
    mass_fraction_c2 = if_else(
      is.na(`Mass fraction c2 phase_2`),
      1 - `Mass fraction c1 phase_2`,
      `Mass fraction c2 phase_2`
    ),
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      !is.na(`Molality, mol/kg c1 phase_2`),
      `Molality, mol/kg c1 phase_2` * molweight1 / 1000 / (
        1 + `Molality, mol/kg c1 phase_2` * molweight1 / 1000
      ),
      mass_fraction_c1
    ),
    mass_fraction_c2 = if_else(
      !is.na(`Molality, mol/kg c2 phase_2`),
      `Molality, mol/kg c2 phase_2` * molweight2 / 1000 / (
        1 + `Molality, mol/kg c2 phase_2` * molweight2 / 1000
      ),
      mass_fraction_c2
    ),
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      is.na(mass_fraction_c1),
      1 - mass_fraction_c2,
      mass_fraction_c1
    ),
    mass_fraction_c2 = if_else(
      is.na(mass_fraction_c2),
      1 - mass_fraction_c1,
      mass_fraction_c2
    ),
  ) %>%
  mutate(
    mole_fraction_c1 = if_else(
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2),
      (
        mass_fraction_c1 /
          molweight1
      ) /
        (
          mass_fraction_c1 / molweight1 +
            mass_fraction_c2 / molweight2
        ),
      mole_fraction_c1
    ),
    mole_fraction_c2 = if_else(
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2),
      (
        mass_fraction_c2 /
          molweight2
      ) /
        (
          mass_fraction_c1 / molweight1 +
            mass_fraction_c2 / molweight2
        ),
      mole_fraction_c2
    )
  )

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe %>%
  filter(
    m0_phase_2 < 0.0
  ) %>%
  view()

tmlframe <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1)
  ) %>%
  filter(
    m0_phase_2 > 0.0
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))


### checking molecules available


## Save

tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "vp_binary.parquet"
  )

tml_saved <- read_parquet("vp_binary.parquet")
tml_saved %>% colnames()
