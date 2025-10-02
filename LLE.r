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
    type == "Liquid-liquid equilibrium temperature, K",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Liquid-liquid equilibrium temperature, K",
    is.na(c3),
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

## analysis

tmlframe %>% nrow()
tmlframe %>%
  summary()


### Fill in missing mole fraction info

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1p1 = if_else(
      is.na(`Mole fraction c1 phase_1`),
      1 - `Mole fraction c2 phase_1`,
      `Mole fraction c1 phase_1`
    ),
    mole_fraction_c2p1 = if_else(
      is.na(`Mole fraction c2 phase_1`),
      1 - `Mole fraction c1 phase_1`,
      `Mole fraction c2 phase_1`
    ),
    mole_fraction_c1p2 = if_else(
      is.na(`Mole fraction c1 phase_2`),
      1 - `Mole fraction c2 phase_2`,
      `Mole fraction c1 phase_2`
    ),
    mole_fraction_c2p2 = if_else(
      is.na(`Mole fraction c2 phase_2`),
      1 - `Mole fraction c1 phase_2`,
      `Mole fraction c2 phase_2`
    )
  ) %>%
  mutate(
    mole_fraction_c1 = if_else(
      is.na(mole_fraction_c1p1),
      mole_fraction_c1p2,
      mole_fraction_c1p1
    ),
    mole_fraction_c2 = if_else(
      is.na(mole_fraction_c2p1),
      mole_fraction_c2p2,
      mole_fraction_c2p1
    )
  )

### Fill in missing mass fraction info

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1 = if_else(
      is.na(`Mass fraction c1 phase_1`),
      1 - `Mass fraction c2 phase_1`,
      `Mass fraction c1 phase_1`
    ),
    mass_fraction_c2 = if_else(
      is.na(`Mass fraction c2 phase_1`),
      1 - `Mass fraction c1 phase_1`,
      `Mass fraction c2 phase_1`
    ),
  )

tmlframe <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1) |
      !is.na(mass_fraction_c1)
  )

### filter missing in c2 (nematic liquid crystal)

tmlframe <- tmlframe %>%
  filter(
    !is.na(c2)
  )


### checking phases

tmlframe %>%
  group_by(phase_1, phase_2, m0_phase) %>%
  summarise(n = n()) %>%
  arrange(desc(n))


tmlframe %>%
  group_by(type, m0_phase) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

### filter columns

tmlframe <- tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking molecules available

tmlframe %>%
  distinct(inchi1, inchi2) %>%
  nrow()

tmlframe %>%
  filter(
    (
      grepl("ammonium", c1, ignore.case = TRUE) |
        grepl("ammonium", c2, ignore.case = TRUE)
    )
  ) %>%
  view()

tmlframe %>%
  filter(
    (
      grepl("imidazolium", c1, ignore.case = TRUE) |
        grepl("imidazolium", c2, ignore.case = TRUE)
    )
  ) %>%
  view()

## Save

tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "lle_binary.parquet"
  )

tml_saved <- read_parquet("lle_binary.parquet")
tml_saved %>% colnames()
