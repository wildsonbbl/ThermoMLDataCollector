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
  group_by(
    type
  ) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  print(n = 25)

## selecting properties of interest

tmlset %>%
  filter(
    type == "Mass fraction",
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  print(n = 26)

tmlframe <- tmlset %>%
  filter(
    type == "Mass fraction",
    !is.na(c3),
    grepl("Liquid", phase_1, ignore.case = TRUE),
    grepl("Liquid", phase_2, ignore.case = TRUE),
    is.na(phase_3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### merge temperature and pressure

tmlframe <- tmlframe %>%
  mutate(
    T_K = if_else(
      is.na(`Temperature, K phase_1`),
      `Temperature, K phase_2`,
      `Temperature, K phase_1`
    ),
    P_kPa = if_else(
      is.na(`Pressure, kPa phase_1`),
      `Pressure, kPa phase_2`,
      `Pressure, kPa phase_1`
    )
  )

### get mass fraction

tmlframe %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1p1 = case_when(
      !is.na(m1_phase_1) ~ m1_phase_1,
      !is.na(`Mass fraction c1 phase_1`) ~ `Mass fraction c1 phase_1`,
    ),
    mass_fraction_c2p1 = case_when(
      !is.na(m2_phase_1) ~ m2_phase_1,
      !is.na(`Mass fraction c2 phase_1`) ~ `Mass fraction c2 phase_1`,
      !is.na(`Solvent: Mass fraction c2 phase_1`) & !is.na(`m1_phase_1`) ~
        `Solvent: Mass fraction c2 phase_1` * (1 - `m1_phase_1`),
      !is.na(`Solvent: Mass fraction c2 phase_1`) & !is.na(`m3_phase_1`) ~
        `Solvent: Mass fraction c2 phase_1` * (1 - `m3_phase_1`),
    ),
    mass_fraction_c3p1 = case_when(
      !is.na(m3_phase_1) ~ m3_phase_1,
      !is.na(`Mass fraction c3 phase_1`) ~ `Mass fraction c3 phase_1`,
    ),
    mass_fraction_c1p2 = case_when(
      !is.na(m1_phase_2) ~ m1_phase_2,
      !is.na(`Mass fraction c1 phase_2`) ~ `Mass fraction c1 phase_2`,
    ),
    mass_fraction_c2p2 = case_when(
      !is.na(m2_phase_2) ~ m2_phase_2,
      !is.na(`Mass fraction c2 phase_2`) ~ `Mass fraction c2 phase_2`,
      !is.na(`Solvent: Mass fraction c2 phase_2`) & !is.na(`m1_phase_2`) ~
        `Solvent: Mass fraction c2 phase_2` * (1 - `m1_phase_2`),
      !is.na(`Solvent: Mass fraction c2 phase_2`) & !is.na(`m3_phase_2`) ~
        `Solvent: Mass fraction c2 phase_2` * (1 - `m3_phase_2`),
    ),
    mass_fraction_c3p2 = case_when(
      !is.na(m3_phase_2) ~ m3_phase_2,
      !is.na(`Mass fraction c3 phase_2`) ~ `Mass fraction c3 phase_2`,
    )
  ) %>%
  mutate(
    mass_fraction_c1p1 = case_when(
      !is.na(mass_fraction_c1p1) ~ mass_fraction_c1p1,
      !is.na(mass_fraction_c2p1) & !is.na(mass_fraction_c3p1) ~ 1 - mass_fraction_c2p1 - mass_fraction_c3p1
    ),
    mass_fraction_c2p1 = case_when(
      !is.na(mass_fraction_c2p1) ~ mass_fraction_c2p1,
      !is.na(mass_fraction_c1p1) & !is.na(mass_fraction_c3p1) ~ 1 - mass_fraction_c1p1 - mass_fraction_c3p1
    ),
    mass_fraction_c3p1 = case_when(
      !is.na(mass_fraction_c3p1) ~ mass_fraction_c3p1,
      !is.na(mass_fraction_c2p1) & !is.na(mass_fraction_c1p1) ~ 1 - mass_fraction_c2p1 - mass_fraction_c1p1
    ),
    mass_fraction_c1p2 = case_when(
      !is.na(mass_fraction_c1p2) ~ mass_fraction_c1p2,
      !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c3p2) ~ 1 - mass_fraction_c2p2 - mass_fraction_c3p2
    ),
    mass_fraction_c2p2 = case_when(
      !is.na(mass_fraction_c2p2) ~ mass_fraction_c2p2,
      !is.na(mass_fraction_c1p2) & !is.na(mass_fraction_c3p2) ~ 1 - mass_fraction_c1p2 - mass_fraction_c3p2
    ),
    mass_fraction_c3p2 = case_when(
      !is.na(mass_fraction_c3p2) ~ mass_fraction_c3p2,
      !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c1p2) ~ 1 - mass_fraction_c2p2 - mass_fraction_c1p2
    )
  )

### get mole fraction

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1p1 = case_when(
      !is.na(mass_fraction_c1p1) & !is.na(mass_fraction_c2p1) & !is.na(mass_fraction_c3p1) ~ (
        (mass_fraction_c1p1 / molweight1) /
          (mass_fraction_c1p1 / molweight1 + mass_fraction_c2p1 / molweight2 + mass_fraction_c3p1 / molweight3)
      ),
    ),
    mole_fraction_c2p1 = case_when(
      !is.na(mass_fraction_c1p1) & !is.na(mass_fraction_c2p1) & !is.na(mass_fraction_c3p1) ~ (
        (mass_fraction_c2p1 / molweight2) /
          (mass_fraction_c1p1 / molweight1 + mass_fraction_c2p1 / molweight2 + mass_fraction_c3p1 / molweight3)
      ),
    ),
    mole_fraction_c3p1 = case_when(
      !is.na(mass_fraction_c1p1) & !is.na(mass_fraction_c2p1) & !is.na(mass_fraction_c3p1) ~ (
        (mass_fraction_c3p1 / molweight3) /
          (mass_fraction_c1p1 / molweight1 + mass_fraction_c2p1 / molweight2 + mass_fraction_c3p1 / molweight3)
      ),
    ),
    mole_fraction_c1p2 = case_when(
      !is.na(mass_fraction_c1p2) & !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c3p2) ~ (
        (mass_fraction_c1p2 / molweight1) /
          (mass_fraction_c1p2 / molweight1 + mass_fraction_c2p2 / molweight2 + mass_fraction_c3p2 / molweight3)
      ),
    ),
    mole_fraction_c2p2 = case_when(
      !is.na(mass_fraction_c1p2) & !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c3p2) ~ (
        (mass_fraction_c2p2 / molweight2) /
          (mass_fraction_c1p2 / molweight1 + mass_fraction_c2p2 / molweight2 + mass_fraction_c3p2 / molweight3)
      ),
    ),
    mole_fraction_c3p2 = case_when(
      !is.na(mass_fraction_c1p2) & !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c3p2) ~ (
        (mass_fraction_c3p2 / molweight3) /
          (mass_fraction_c1p2 / molweight1 + mass_fraction_c2p2 / molweight2 + mass_fraction_c3p2 / molweight3)
      ),
    )
  )

## Check distinct rows

tmlframe <- tmlframe %>%
  distinct(
    inchi1, inchi2, inchi3, T_K, P_kPa,
    mole_fraction_c1p1, mole_fraction_c2p1, mole_fraction_c3p1,
    mole_fraction_c1p2, mole_fraction_c2p2, mole_fraction_c3p2,
    .keep_all = TRUE
  )

### merge mole fractions

tml_p1 <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1p1),
    !is.na(mole_fraction_c2p1),
    !is.na(mole_fraction_c3p1)
  ) %>%
  rename(
    mole_fraction_c1 = mole_fraction_c1p1,
    mole_fraction_c2 = mole_fraction_c2p1,
    mole_fraction_c3 = mole_fraction_c3p1
  )

tml_p2 <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1p2),
    !is.na(mole_fraction_c2p2),
    !is.na(mole_fraction_c3p2)
  ) %>%
  rename(
    mole_fraction_c1 = mole_fraction_c1p2,
    mole_fraction_c2 = mole_fraction_c2p2,
    mole_fraction_c3 = mole_fraction_c3p2
  )

tml_combined <- bind_rows(tml_p1, tml_p2) %>%
  select(where(~ !all(is.na(.x)))) %>%
  filter(
    between(mole_fraction_c1, -1e-5, 1 + 1e-5),
    between(mole_fraction_c2, -1e-5, 1 + 1e-5),
    between(mole_fraction_c3, -1e-5, 1 + 1e-5),
  )

## Save

tml_combined %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "lle_mass_ternary.parquet"
  )

tml_saved <- read_parquet("lle_mass_ternary.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()
