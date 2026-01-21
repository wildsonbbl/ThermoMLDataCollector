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

tml_binary <- tmlset %>%
  filter(
    type == "Mole fraction",
    is.na(c3),
    (inchi1 == "InChI=1S/CO2/c2-1-3" | inchi2 == "InChI=1S/CO2/c2-1-3")
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking phases

tml_binary <- tml_binary %>%
  filter(
    phase_1 == "Gas",
    phase_2 == "Liquid"
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### Fill in missing mole fraction info

tml_binary %>% colnames()

tml_binary <- tml_binary %>%
  mutate(
    mole_fraction_c1p1 = case_when(
      !is.na(m1_phase_1) ~ m1_phase_1,
      is.na(m1_phase_1) & !is.na(m2_phase_1) ~ 1 - m2_phase_1,
      !is.na(`Mole fraction c1 phase_1`) ~ `Mole fraction c1 phase_1`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ 1.0,
      !is.na(`Pressure, kPa c2 phase_1`) ~ 0.0,
      inchi1 == "InChI=1S/CO2/c2-1-3" ~ 1.0,
      TRUE ~ 0.0
    ),
    mole_fraction_c2p1 = case_when(
      !is.na(m2_phase_1) ~ m2_phase_1,
      is.na(m2_phase_1) & !is.na(m1_phase_1) ~ 1 - m1_phase_1,
      !is.na(`Mole fraction c2 phase_1`) ~ `Mole fraction c2 phase_1`,
      !is.na(`Pressure, kPa c2 phase_1`) ~ 1.0,
      !is.na(`Pressure, kPa c1 phase_1`) ~ 0.0,
      inchi2 == "InChI=1S/CO2/c2-1-3" ~ 1.0,
      TRUE ~ 0.0
    ),
    mole_fraction_c1p2 = case_when(
      !is.na(m1_phase_2) ~ m1_phase_2,
      !is.na(`Mole fraction c1 phase_2`) ~ `Mole fraction c1 phase_2`,
      TRUE ~ NA,
    ),
    mole_fraction_c2p2 = case_when(
      !is.na(m2_phase_2) ~ m2_phase_2,
      !is.na(`Mole fraction c2 phase_2`) ~ `Mole fraction c2 phase_2`,
      TRUE ~ NA,
    ),
  ) %>%
  mutate(
    mole_fraction_c1p1 = if_else(
      is.na(mole_fraction_c1p1) & !is.na(mole_fraction_c2p1),
      1 - mole_fraction_c2p1,
      mole_fraction_c1p1
    ),
    mole_fraction_c1p2 = if_else(
      is.na(mole_fraction_c1p2) & !is.na(mole_fraction_c2p2),
      1 - mole_fraction_c2p2,
      mole_fraction_c1p2
    ),
    mole_fraction_c2p1 = if_else(
      !is.na(mole_fraction_c1p1) & is.na(mole_fraction_c2p1),
      1 - mole_fraction_c1p1,
      mole_fraction_c2p1
    ),
    mole_fraction_c2p2 = if_else(
      !is.na(mole_fraction_c1p2) & is.na(mole_fraction_c2p2),
      1 - mole_fraction_c1p2,
      mole_fraction_c2p2
    ),
  )


### merge temperature and pressure

tml_binary %>%
  mutate(
    T_K = if_else(
      is.na(`Temperature, K phase_1`),
      `Temperature, K phase_2`,
      `Temperature, K phase_1`
    ),
    P_kPa = case_when(
      !is.na(`Pressure, kPa phase_2`) ~ `Pressure, kPa phase_2`,
      !is.na(`Pressure, kPa phase_1`) ~ `Pressure, kPa phase_1`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ `Pressure, kPa c1 phase_1`,
      !is.na(`Pressure, kPa c2 phase_1`) ~ `Pressure, kPa c2 phase_1`
    )
  ) %>%
  filter(
    is.na(P_kPa)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tml_binary %>%
  filter(is.na(mole_fraction_c1p2)) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tml_binary <- tml_binary %>%
  mutate(
    T_K = if_else(
      is.na(`Temperature, K phase_1`),
      `Temperature, K phase_2`,
      `Temperature, K phase_1`
    ),
    P_kPa = case_when(
      !is.na(`Pressure, kPa phase_2`) ~ `Pressure, kPa phase_2`,
      !is.na(`Pressure, kPa phase_1`) ~ `Pressure, kPa phase_1`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ `Pressure, kPa c1 phase_1`,
      !is.na(`Pressure, kPa c2 phase_1`) ~ `Pressure, kPa c2 phase_1`
    )
  ) %>%
  filter(
    !is.na(P_kPa),
    !is.na(mole_fraction_c1p2)
  ) %>%
  select(where(~ !all(is.na(.x))))


### checking molecules available

tml_binary %>%
  filter(
    c1 == "carbon dioxide",
    c2 == "water",
  ) %>%
  select(c1, c2, T_K, P_kPa, mole_fraction_c1p1, mole_fraction_c2p1, mole_fraction_c1p2, mole_fraction_c2p2) %>%
  summary()

tml_binary %>%
  filter(
    c1 == "carbon dioxide",
    c2 == "ethanol",
  ) %>%
  select(c1, c2, T_K, P_kPa, mole_fraction_c1p1, mole_fraction_c2p1, mole_fraction_c1p2, mole_fraction_c2p2) %>%
  summary()

tml_binary %>%
  filter(
    (
      grepl("ammonium", c1, ignore.case = TRUE) |
        grepl("ammonium", c2, ignore.case = TRUE)
    )
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tml_binary %>%
  filter(
    (
      grepl("choline", c1, ignore.case = TRUE) |
        grepl("choline", c2, ignore.case = TRUE)
    )
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tml_binary %>%
  filter(
    (
      grepl("amine", c1, ignore.case = TRUE) |
        grepl("amine", c2, ignore.case = TRUE)
    )
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tml_binary %>%
  filter(
    (
      grepl("imidazolium", c1, ignore.case = TRUE) |
        grepl("imidazolium", c2, ignore.case = TRUE)
    )
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

## Save

tml_binary %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "co2_binary.parquet"
  )

tml_saved <- read_parquet("co2_binary.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()
