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
  group_by(phase_1, phase_2) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlset %>%
  filter(
    type == "Liquid-liquid equilibrium temperature, K",
    is.na(c3),
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### merge temperature and pressure

tmlframe <- tmlframe %>%
  mutate(
    T_K = case_when(
      !is.na(m0_phase_1) ~ m0_phase_1,
      !is.na(m0_phase_2) ~ m0_phase_2
    ),
    P_kPa = case_when(
      !is.na(`Pressure, kPa phase_1`) ~ `Pressure, kPa phase_1`,
      !is.na(`Pressure, kPa phase_2`) ~ `Pressure, kPa phase_2`,
    )
  )

### get mass fraction

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1p1 = case_when(
      !is.na(`Mass fraction c1 phase_1`) ~ `Mass fraction c1 phase_1`,
      !is.na(`Mass fraction c2 phase_1`) ~ 1 - `Mass fraction c2 phase_1`,
      !is.na(`Molality, mol/kg c1 phase_1`) ~
        `Molality, mol/kg c1 phase_1` /
          (`Molality, mol/kg c1 phase_1` + 1000 / molweight2)
    ),
    mass_fraction_c2p1 = case_when(
      !is.na(`Mass fraction c1 phase_1`) ~ 1 - `Mass fraction c1 phase_1`,
      !is.na(`Mass fraction c2 phase_1`) ~ `Mass fraction c2 phase_1`,
      !is.na(`Molality, mol/kg c1 phase_1`) ~
        1000 / molweight2 /
          (`Molality, mol/kg c1 phase_1` + 1000 / molweight2)
    )
  )

### get mole fraction

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1p1 = case_when(
      !is.na(`Mole fraction c1 phase_1`) ~ `Mole fraction c1 phase_1`,
      !is.na(`Mole fraction c2 phase_1`) ~ 1 - `Mole fraction c2 phase_1`,
      !is.na(mass_fraction_c1p1) & !is.na(mass_fraction_c2p1) ~ (
        (mass_fraction_c1p1 / molweight1) /
          (mass_fraction_c1p1 / molweight1 + mass_fraction_c2p1 / molweight2)
      ),
    ),
    mole_fraction_c2p1 = case_when(
      !is.na(`Mole fraction c1 phase_1`) ~ 1 - `Mole fraction c1 phase_1`,
      !is.na(`Mole fraction c2 phase_1`) ~ `Mole fraction c2 phase_1`,
      !is.na(mass_fraction_c1p1) & !is.na(mass_fraction_c2p1) ~ (
        (mass_fraction_c2p1 / molweight2) /
          (mass_fraction_c1p1 / molweight1 + mass_fraction_c2p1 / molweight2)
      ),
    ),
    mole_fraction_c1p2 = case_when(
      !is.na(`Mole fraction c1 phase_2`) ~ `Mole fraction c1 phase_2`,
      !is.na(`Mole fraction c2 phase_2`) ~ 1 - `Mole fraction c2 phase_2`,
    ),
    mole_fraction_c2p2 = case_when(
      !is.na(`Mole fraction c1 phase_2`) ~ 1 - `Mole fraction c1 phase_2`,
      !is.na(`Mole fraction c2 phase_2`) ~ `Mole fraction c2 phase_2`,
    ),
  )

### merge mole fractions

tml_p1 <- tmlframe %>%
  filter(!is.na(mole_fraction_c1p1), !is.na(mole_fraction_c2p1)) %>%
  rename(
    mole_fraction_c1 = mole_fraction_c1p1,
    mole_fraction_c2 = mole_fraction_c2p1
  )

tml_p2 <- tmlframe %>%
  filter(!is.na(mole_fraction_c1p2), !is.na(mole_fraction_c2p2)) %>%
  rename(
    mole_fraction_c1 = mole_fraction_c1p2,
    mole_fraction_c2 = mole_fraction_c2p2
  )

tml_combined <- bind_rows(tml_p1, tml_p2)

tml_combined %>% summary()

### checking molecules available

tml_combined %>%
  distinct(inchi1, inchi2) %>%
  nrow()

tml_combined %>%
  filter(
    (
      grepl("ammonium", c1, ignore.case = TRUE) |
        grepl("ammonium", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

tml_combined %>%
  filter(
    (
      grepl("choline", c1, ignore.case = TRUE) |
        grepl("choline", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

tml_combined %>%
  filter(
    (
      grepl("amine", c1, ignore.case = TRUE) |
        grepl("amine", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

tml_combined %>%
  filter(
    (
      grepl("imidazolium", c1, ignore.case = TRUE) |
        grepl("imidazolium", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

## Save

tml_combined %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "lle_binary_temp.parquet"
  )

tml_saved <- read_parquet("lle_binary_temp.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()
