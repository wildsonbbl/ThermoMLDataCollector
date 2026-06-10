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
    type == "Mole fraction",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlset %>%
  filter(
    type == "Mass fraction",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlset %>%
  filter(
    type == "Mass fraction",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlset %>%
  filter(
    type == "Mole fraction",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlframe %>%
  filter(
    phase_1 == "Gas",
    phase_2 == "Liquid",
    is.na(phase_3)
  ) %>%
  select(where(~ !all(is.na(.x))))

### Fill in missing mole fraction info

tmlframe %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1p1 = case_when(
      !is.na(m1_phase_1) ~ m1_phase_1,
      !is.na(m2_phase_1) ~ 1 - m2_phase_1,
      !is.na(`Mole fraction c1 phase_1`) ~ `Mole fraction c1 phase_1`,
      !is.na(`Mole fraction c2 phase_1`) ~ 1 - `Mole fraction c2 phase_1`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ 1.0,
      !is.na(`Pressure, kPa c2 phase_1`) ~ 0.0
    ),
    mole_fraction_c1p2 = case_when(
      !is.na(m1_phase_2) ~ m1_phase_2,
      !is.na(m2_phase_2) ~ 1 - m2_phase_2,
      !is.na(`Mole fraction c1 phase_2`) ~ `Mole fraction c1 phase_2`,
      !is.na(`Mole fraction c2 phase_2`) ~ 1 - `Mole fraction c2 phase_2`
    ),
    mole_fraction_c2p1 = case_when(
      !is.na(m2_phase_1) ~ m2_phase_1,
      !is.na(m1_phase_1) ~ 1 - m1_phase_1,
      !is.na(`Mole fraction c2 phase_1`) ~ `Mole fraction c2 phase_1`,
      !is.na(`Mole fraction c1 phase_1`) ~ 1 - `Mole fraction c1 phase_1`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ 0.0,
      !is.na(`Pressure, kPa c2 phase_1`) ~ 1.0
    ),
    mole_fraction_c2p2 = case_when(
      !is.na(m2_phase_2) ~ m2_phase_2,
      !is.na(m1_phase_2) ~ 1 - m1_phase_2,
      !is.na(`Mole fraction c2 phase_2`) ~ `Mole fraction c2 phase_2`,
      !is.na(`Mole fraction c1 phase_2`) ~ 1 - `Mole fraction c1 phase_2`
    )
  )

tmlframe %>% summary()

### merge temperature and pressure

tmlframe <- tmlframe %>%
  mutate(
    T_K = case_when(
      !is.na(`Temperature, K phase_1`) ~ `Temperature, K phase_1`,
      !is.na(`Temperature, K phase_2`) ~ `Temperature, K phase_2`,
    ),
    P_kPa = case_when(
      !is.na(`Pressure, kPa phase_1`) ~ `Pressure, kPa phase_1`,
      !is.na(`Pressure, kPa phase_2`) ~ `Pressure, kPa phase_2`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ `Pressure, kPa c1 phase_1`,
      !is.na(`Pressure, kPa c2 phase_1`) ~ `Pressure, kPa c2 phase_1`,
    )
  )

tmlframe <- tmlframe %>%
  filter(!is.na(T_K), !is.na(P_kPa))

tmlframe %>% summary()

## Check distinct rows

tmlframe <- tmlframe %>%
  distinct(
    inchi1, inchi2, T_K, P_kPa,
    mole_fraction_c1p2, mole_fraction_c2p2,
    mole_fraction_c1p1, mole_fraction_c2p1,
    .keep_all = TRUE
  )

## Save

tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "vle_binary.parquet"
  )

tml_saved <- read_parquet("vle_binary.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()

### checking molecules available

tml_saved %>%
  filter(inchi1 == "InChI=1S/CO2/c2-1-3" | inchi2 == "InChI=1S/CO2/c2-1-3") %>%
  summary()

tml_saved %>%
  distinct(inchi1, inchi2) %>%
  nrow()

tml_saved %>%
  filter(
    (
      grepl("ammonium", c1, ignore.case = TRUE) |
        grepl("ammonium", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

tml_saved %>%
  filter(
    (
      grepl("choline", c1, ignore.case = TRUE) |
        grepl("choline", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

tml_saved %>%
  filter(
    (
      grepl("amine", c1, ignore.case = TRUE) |
        grepl("amine", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

tml_saved %>%
  filter(
    (
      grepl("imidazolium", c1, ignore.case = TRUE) |
        grepl("imidazolium", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()
