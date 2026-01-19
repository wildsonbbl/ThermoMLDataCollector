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
    type == "Surface tension liquid-gas, N/m",
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Surface tension liquid-gas, N/m",
    !is.na(c2),
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking and filtering phases

tmlframe %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames() %>%
  sort()

tmlframe %>%
  group_by(phase_1, phase_2) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

### Fill in missing mass fraction info

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1 = case_when(
      !is.na(`Mass fraction c1 phase_2`) ~ `Mass fraction c1 phase_2`,
      !is.na(`Mass fraction c2 phase_2`) ~ 1 - `Mass fraction c2 phase_2`,
      !is.na(`Molality, mol/kg c1 phase_2`) ~ `Molality, mol/kg c1 phase_2` * molweight1 / 1000,
      !is.na(`Mass ratio of solute to solvent c1 phase_2`) ~
        `Mass ratio of solute to solvent c1 phase_2` / (1 + `Mass ratio of solute to solvent c1 phase_2`),
    ),
    mass_fraction_c2 = case_when(
      !is.na(`Mass fraction c2 phase_2`) ~ `Mass fraction c2 phase_2`,
      !is.na(`Mass fraction c1 phase_2`) ~ 1 - `Mass fraction c1 phase_2`,
      !is.na(`Molality, mol/kg c1 phase_2`) ~ 1 - `Molality, mol/kg c1 phase_2` * molweight1 / 1000,
      !is.na(`Mass ratio of solute to solvent c1 phase_2`) ~
        1 / (1 + `Mass ratio of solute to solvent c1 phase_2`),
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
  )

### Fill in missing mole fraction info

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1 = case_when(
      !is.na(`Mole fraction c1 phase_2`) ~ `Mole fraction c1 phase_2`,
      !is.na(`Mole fraction c2 phase_2`) ~ 1 - `Mole fraction c2 phase_2`,
      !is.na(`Amount ratio of solute to solvent c1 phase_2`) ~
        `Amount ratio of solute to solvent c1 phase_2` / (1 + `Amount ratio of solute to solvent c1 phase_2`),
      !is.na(`Amount ratio of solute to solvent c2 phase_2`) ~
        1 / (1 + `Amount ratio of solute to solvent c2 phase_2`),
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) ~ (
        (mass_fraction_c1 / molweight1) /
          (mass_fraction_c1 / molweight1 + mass_fraction_c2 / molweight2)
      ),
    ),
    mole_fraction_c2 = case_when(
      !is.na(`Mole fraction c2 phase_2`) ~ `Mole fraction c2 phase_2`,
      !is.na(`Mole fraction c1 phase_2`) ~ 1 - `Mole fraction c1 phase_2`,
      !is.na(`Amount ratio of solute to solvent c2 phase_2`) ~
        `Amount ratio of solute to solvent c2 phase_2` / (1 + `Amount ratio of solute to solvent c2 phase_2`),
      !is.na(`Amount ratio of solute to solvent c1 phase_2`) ~
        1 / (1 + `Amount ratio of solute to solvent c1 phase_2`),
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) ~ (
        (mass_fraction_c2 / molweight2) /
          (mass_fraction_c1 / molweight1 + mass_fraction_c2 / molweight2)
      ),
    )
  ) %>%
  mutate(
    mole_fraction_c1 = if_else(
      is.na(mole_fraction_c1) & !is.na(mole_fraction_c2),
      1 - mole_fraction_c2,
      mole_fraction_c1
    ),
    mole_fraction_c2 = if_else(
      is.na(mole_fraction_c2) & !is.na(mole_fraction_c1),
      1 - mole_fraction_c1,
      mole_fraction_c2
    ),
  )



### filter concentrations

tmlframe %>%
  filter(
    is.na(mole_fraction_c1)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking molecules available

tmlframe %>%
  filter(
    is.na(`Temperature, K phase_2`)
  ) %>%
  view()

tmlframe <- tmlframe %>%
  mutate(
    T_K = case_when(
      !is.na(`Temperature, K phase_2`) ~ `Temperature, K phase_2`,
      is.na(`Temperature, K phase_2`) ~ 273.15 + 25.0,
    ),
  )

## Save

tmlframe %>% summary()

tmlframe %>%
  rename(
    st = m0_phase_2,
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "st_binary.parquet"
  )

tml_saved <- read_parquet("st_binary.parquet")
tml_saved %>% colnames()

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
  )
