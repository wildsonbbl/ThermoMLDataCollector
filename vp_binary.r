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

## analysis

tmlframe %>% nrow()
tmlframe %>%
  summary()

### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3, phase_4, m0_phase) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  print(n = 31)


tmlframe %>%
  group_by(m0_phase) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlframe %>%
  filter(
    m0_phase == "Liquid",
    phase_1 == "Gas",
    phase_2 == "Liquid",
    is.na(phase_3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

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
  )

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1) |
      !is.na(mass_fraction_c1) |
      !is.na(`Molality, mol/kg c1 phase_2`) |
      !is.na(`Molality, mol/kg c2 phase_2`)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  view()



tmlframe <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1) |
      !is.na(mass_fraction_c1) |
      !is.na(`Molality, mol/kg c1 phase_2`) |
      !is.na(`Molality, mol/kg c2 phase_2`)
  ) %>%
  filter(
    m0 > 0
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

tmlframe %>%
  summary()

### checking molecules available
tmlframe %>%
  filter(
    m0 < 0.001
  ) %>%
  view()

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
