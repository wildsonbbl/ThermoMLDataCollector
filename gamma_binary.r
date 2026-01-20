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
    type == "Activity coefficient",
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Activity coefficient",
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
    phase_1 != "Crystal" & phase_1 != "Glass"
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
      !is.na(`Mass fraction c1 phase_1`) ~ `Mass fraction c1 phase_1`,
      !is.na(`Molality, mol/kg c1 phase_1`) ~
        `Molality, mol/kg c1 phase_1` * molweight1 / 1000 / (
          1 + `Molality, mol/kg c1 phase_1` * molweight1 / 1000
        ),
      !is.na(`Mass fraction c2 phase_1`) ~ 1 - `Mass fraction c2 phase_1`,
      !is.na(`Mass fraction c2 phase_2`) ~ 1 - `Mass fraction c2 phase_2`
    ),
    mass_fraction_c2 = case_when(
      !is.na(`Mass fraction c1 phase_1`) ~ 1 - `Mass fraction c1 phase_1`,
      !is.na(`Molality, mol/kg c1 phase_1`) ~
        1 / (
          1 + `Molality, mol/kg c1 phase_1` * molweight1 / 1000
        ),
      !is.na(`Mass fraction c2 phase_1`) ~ `Mass fraction c2 phase_1`,
      !is.na(`Mass fraction c2 phase_2`) ~ `Mass fraction c2 phase_2`
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
  filter(
    is.na(mole_fraction_c1) | is.na(mole_fraction_c2)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tmlframe <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1), !is.na(mole_fraction_c2)
  ) %>%
  select(where(~ !all(is.na(.x))))

tmlframe %>%
  mutate(
    T_K = case_when(
      !is.na(`Temperature, K phase_1`) ~ `Temperature, K phase_1`,
      !is.na(`Temperature, K phase_2`) ~ `Temperature, K phase_2`,
    ),
    P_kPa = case_when(
      !is.na(`Pressure, kPa phase_1`) ~ `Pressure, kPa phase_1`,
      is.na(`Pressure, kPa phase_1`) ~ 101.325
    ),
    m1 = case_when(
      !is.na(m1_phase_1) ~ m1_phase_1,
      !is.na(m1_phase_2) ~ m1_phase_2
    ),
    m2 = case_when(
      !is.na(m2_phase_1) ~ m2_phase_1,
      !is.na(m2_phase_2) ~ m2_phase_2
    ),
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "gamma_binary.parquet"
  )

tml_saved <- read_parquet("gamma_binary.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()
