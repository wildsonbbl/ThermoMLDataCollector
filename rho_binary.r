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
    type == "Mass density, kg/m3",
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Mass density, kg/m3",
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe %>%
  filter(
    phase_1 == "Liquid",
    is.na(phase_2)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tmlframe <- tmlframe %>%
  filter(
    phase_1 == "Liquid",
    is.na(phase_2)
  ) %>%
  select(where(~ !all(is.na(.x))))


### Fill in missing mole fraction info

tmlframe %>%
  filter(!is.na(
    `Molality, mol/kg c1 phase_1`
  )) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(matches(c("c[1-3] phase_[1-2]", "Solvent"))) %>%
  colnames()


tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(matches(c("c[1-3] phase_[1-2]", "Solvent"))) %>%
  colnames()

tmlframe %>%
  filter(!is.na(
    `Molality, mol/kg c2 phase_1`
  )) %>%
  group_by(
    `Solvent for m0_phase_1`
  ) %>%
  summarise(n = n())

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1 = if_else(
      is.na(`Mole fraction c1 phase_1`),
      1 - `Mole fraction c2 phase_1`,
      `Mole fraction c1 phase_1`
    ),
    mole_fraction_c2 = if_else(
      is.na(`Mole fraction c2 phase_1`),
      1 - `Mole fraction c1 phase_1`,
      `Mole fraction c2 phase_1`
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
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      !is.na(`Molality, mol/kg c1 phase_1`),
      `Molality, mol/kg c1 phase_1` * molweight1 / 1000 / (
        1 + `Molality, mol/kg c1 phase_1` * molweight1 / 1000
      ),
      mass_fraction_c1
    ),
    mass_fraction_c2 = if_else(
      !is.na(`Molality, mol/kg c2 phase_1`),
      `Molality, mol/kg c2 phase_1` * molweight2 / 1000 / (
        1 + `Molality, mol/kg c2 phase_1` * molweight2 / 1000
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

### filter concentrations

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tmlframe <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1)
  ) %>%
  select(where(~ !all(is.na(.x))))

tmlframe %>%
  filter(!is.na(`Solvent for m0_phase_1`)) %>%
  summary()


### checking molecules available

tmlframe %>%
  filter(
    `Temperature, K phase_1` > 600
  ) %>%
  summary()

tmlframe %>%
  filter(
    `Pressure, kPa phase_1` > 1000
  ) %>%
  summary()

## Save

tmlframe %>% summary()

tmlframe %>%
  rename(
    T_K = `Temperature, K phase_1`,
    rho = m0_phase_1,
    P_kPa = `Pressure, kPa phase_1`
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "rho_binary.parquet"
  )

tml_saved <- read_parquet("rho_binary.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()

tml_saved %>%
  filter(
    c1 != c2,
    (
      grepl("C5H14NO", inchi1, ignore.case = TRUE) |
        grepl("C5H14NO", inchi2, ignore.case = TRUE)
    )
  ) %>%
  group_by(c1, c2, inchi1, inchi2) %>%
  summarise(
    n = n(),
    x_min = min(mole_fraction_c1),
    x_max = max(mole_fraction_c1),
    TK_min = min(T_K),
    TK_max = max(T_K),
    PkPa_min = min(P_kPa),
    PkPa_max = max(P_kPa)
  ) %>%
  write.csv("choline_mix.csv")
