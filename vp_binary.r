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
    type %in% c(
      "Vapor or sublimation pressure, kPa",
      "Boiling temperature at pressure P, K"
    ),
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type %in% c(
      "Vapor or sublimation pressure, kPa",
      "Boiling temperature at pressure P, K"
    ),
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

tmlframe <- tmlframe %>%
  filter(
    phase_1 == "Gas",
    phase_2 == "Liquid",
    is.na(phase_3)
  ) %>%
  select(where(~ !all(is.na(.x))))

tmlframe %>%
  filter(
    m0_phase_2 < 0
  ) %>%
  summary()


### Fill in missing mass fraction info

tmlframe %>% summary()

tmlframe %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()


tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1 = case_when(
      !is.na(`Mass fraction c1 phase_2`) ~ `Mass fraction c1 phase_2`,
      !is.na(`Molality, mol/kg c1 phase_2`) ~
        `Molality, mol/kg c1 phase_2` * molweight1 / 1000 / (1 + `Molality, mol/kg c1 phase_2` * molweight1 / 1000),
      !is.na(`Molality, mol/kg c2 phase_2`) ~
        1 / (1 + `Molality, mol/kg c2 phase_2` * molweight2 / 1000),
      !is.na(`Mass fraction c2 phase_2`) ~ 1 - `Mass fraction c2 phase_2`
    ),
    mass_fraction_c2 = case_when(
      !is.na(`Mass fraction c1 phase_2`) ~ 1 - `Mass fraction c1 phase_2`,
      !is.na(`Molality, mol/kg c1 phase_2`) ~
        1 / (1 + `Molality, mol/kg c1 phase_2` * molweight1 / 1000),
      !is.na(`Molality, mol/kg c2 phase_2`) ~
        `Molality, mol/kg c2 phase_2` * molweight2 / 1000 / (1 + `Molality, mol/kg c2 phase_2` * molweight2 / 1000),
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
      !is.na(`Mole fraction c1 phase_2`) ~ `Mole fraction c1 phase_2`,
      !is.na(`Mole fraction c2 phase_2`) ~ 1 - `Mole fraction c2 phase_2`,
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) ~ (
        (mass_fraction_c1 / molweight1) /
          (mass_fraction_c1 / molweight1 + mass_fraction_c2 / molweight2)
      ),
    ),
    mole_fraction_c2 = case_when(
      !is.na(`Mole fraction c1 phase_2`) ~ 1 - `Mole fraction c1 phase_2`,
      !is.na(`Mole fraction c2 phase_2`) ~ `Mole fraction c2 phase_2`,
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) ~ (
        (mass_fraction_c2 / molweight2) /
          (mass_fraction_c1 / molweight1 + mass_fraction_c2 / molweight2)
      ),
    ),
  )

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1), !is.na(mole_fraction_c2),
    mole_fraction_c1 <= 1, mole_fraction_c1 >= 0,
    mole_fraction_c2 <= 1, mole_fraction_c2 >= 0
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tmlframe %>%
  filter(
    mole_fraction_c1 < 0 | mole_fraction_c2 < 0
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
    !is.na(mole_fraction_c1), !is.na(mole_fraction_c2),
    mole_fraction_c1 <= 1, mole_fraction_c1 >= 0,
    mole_fraction_c2 <= 1, mole_fraction_c2 >= 0
  ) %>%
  select(where(~ !all(is.na(.x))))


## Save

tmlframe %>%
  summary()

tmlframe %>%
  mutate(
    T_K = case_when(
      type == "Vapor or sublimation pressure, kPa" & !is.na(`Temperature, K phase_2`) ~ `Temperature, K phase_2`,
      type == "Vapor or sublimation pressure, kPa" & !is.na(`Temperature, K phase_1`) ~ `Temperature, K phase_1`,
      type == "Boiling temperature at pressure P, K" & !is.na(m0_phase_2) ~ m0_phase_2,
      type == "Boiling temperature at pressure P, K" & !is.na(m0_phase_1) ~ m0_phase_1,
    ),
    BP_kPa = case_when(
      type == "Vapor or sublimation pressure, kPa" & !is.na(m0_phase_2) ~ m0_phase_2,
      type == "Vapor or sublimation pressure, kPa" & !is.na(m0_phase_1) ~ m0_phase_1,
      type == "Boiling temperature at pressure P, K" & !is.na(`Pressure, kPa phase_2`) ~ `Pressure, kPa phase_2`,
      type == "Boiling temperature at pressure P, K" & !is.na(`Pressure, kPa phase_1`) ~ `Pressure, kPa phase_1`
    )
  ) %>%
  filter(
    !is.na(T_K), !is.na(BP_kPa), BP_kPa > 0
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "vp_binary.parquet"
  )

tml_saved <- read_parquet("vp_binary.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()
