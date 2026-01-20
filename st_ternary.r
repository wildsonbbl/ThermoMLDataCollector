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

tmlset %>%
  colnames() %>%
  grep("c[1-3] phase_[1-3]", ., value = TRUE) %>%
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
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Surface tension liquid-gas, N/m",
    !is.na(c3),
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

### Fill in missing mole fraction info

tmlframe %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()

tmlframe <- tmlframe %>%
  filter(
    is.na(`Amount concentration (molarity), mol/dm3 c1 phase_2`),
    is.na(`Amount concentration (molarity), mol/dm3 c2 phase_2`),
    is.na(`Amount concentration (molarity), mol/dm3 c3 phase_2`),
    is.na(`Solvent: Amount concentration (molarity), mol/dm3 c1 phase_2`),
    is.na(`Solvent: Amount concentration (molarity), mol/dm3 c2 phase_2`),
    is.na(`Volume fraction c1 phase_1`),
    is.na(`Volume fraction c2 phase_2`),
    is.na(`Mole fraction c1 phase_1`),
    is.na(`Mole fraction c2 phase_1`),
    is.na(`Solvent: Mole fraction c1 phase_1`)
  ) %>%
  select(where(~ !all(is.na(.x))))

tmlframe %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()

tmlframe %>%
  filter(!is.na(
    `Solvent: Mole fraction c2 phase_2`
  )) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(matches(c("c[1-3] phase_[1-2]"))) %>%
  colnames()

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1 = case_when(
      !is.na(`Mass fraction c1 phase_2`) ~ `Mass fraction c1 phase_2`,
      !is.na(`Mass fraction c2 phase_2`) & !is.na(`Mass fraction c3 phase_2`) ~ 1 - `Mass fraction c2 phase_2` - `Mass fraction c3 phase_2`,
      !is.na(`Mass ratio of solute to solvent c1 phase_2`) ~ (
        `Mass ratio of solute to solvent c1 phase_2` / (1 + `Mass ratio of solute to solvent c1 phase_2`)
      ),
      !is.na(`Molality, mol/kg c1 phase_2`) ~ `Molality, mol/kg c1 phase_2` * molweight1 / 1000,
      !is.na(`Solvent: Mass fraction c1 phase_2`) & !is.na(`Molality, mol/kg c2 phase_2`) ~ (
        (1 - `Molality, mol/kg c2 phase_2` * molweight2 / 1000) * `Solvent: Mass fraction c1 phase_2`
      ),
      !is.na(`Solvent: Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c2 phase_2`) ~ (
        (1 - `Molality, mol/kg c2 phase_2` * molweight2 / 1000) * `Solvent: Molality, mol/kg c1 phase_2` * molweight1 / 1000
      ),
      !is.na(`Solvent: Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c3 phase_2`) ~ (
        (1 - `Molality, mol/kg c3 phase_2` * molweight3 / 1000) * `Solvent: Molality, mol/kg c1 phase_2` * molweight1 / 1000
      )
    ),
    mass_fraction_c2 = case_when(
      !is.na(`Mass fraction c2 phase_2`) ~ `Mass fraction c2 phase_2`,
      !is.na(`Mass fraction c1 phase_2`) & !is.na(`Mass fraction c3 phase_2`) ~ 1 - `Mass fraction c1 phase_2` - `Mass fraction c3 phase_2`,
      !is.na(`Mass ratio of solute to solvent c2 phase_2`) ~ (
        `Mass ratio of solute to solvent c2 phase_2` / (1 + `Mass ratio of solute to solvent c2 phase_2`)
      ),
      !is.na(`Molality, mol/kg c2 phase_2`) ~ `Molality, mol/kg c2 phase_2` * molweight2 / 1000,
      !is.na(`Solvent: Mass fraction c2 phase_2`) & !is.na(`Molality, mol/kg c1 phase_2`) ~ (
        (1 - `Molality, mol/kg c1 phase_2` * molweight1 / 1000) * `Solvent: Mass fraction c2 phase_2`
      ),
      !is.na(`Solvent: Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c3 phase_2`) ~ (
        (1 - `Molality, mol/kg c3 phase_2` * molweight3 / 1000) * (1 - `Solvent: Molality, mol/kg c1 phase_2` * molweight1 / 1000)
      ),
      !is.na(`Solvent: Molality, mol/kg c2 phase_2`) & !is.na(`Molality, mol/kg c1 phase_2`) ~ (
        (1 - `Molality, mol/kg c1 phase_2` * molweight1 / 1000) * (`Solvent: Molality, mol/kg c2 phase_2` * molweight2 / 1000)
      ),
    ),
    mass_fraction_c3 = case_when(
      !is.na(`Mass fraction c3 phase_2`) ~ `Mass fraction c3 phase_2`,
      !is.na(`Mass fraction c1 phase_2`) & !is.na(`Mass fraction c2 phase_2`) ~ 1 - `Mass fraction c1 phase_2` - `Mass fraction c2 phase_2`,
      !is.na(`Molality, mol/kg c3 phase_2`) ~ `Molality, mol/kg c3 phase_2` * molweight3 / 1000,
      !is.na(`Solvent: Mass fraction c1 phase_2`) & !is.na(`Molality, mol/kg c2 phase_2`) ~ (
        (1 - `Molality, mol/kg c2 phase_2` * molweight2 / 1000) * (1 - `Solvent: Mass fraction c1 phase_2`)
      ),
      !is.na(`Solvent: Mass fraction c2 phase_2`) & !is.na(`Molality, mol/kg c1 phase_2`) ~ (
        (1 - `Molality, mol/kg c1 phase_2` * molweight1 / 1000) * (1 - `Solvent: Mass fraction c2 phase_2`)
      ),
      !is.na(`Solvent: Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c2 phase_2`) ~ (
        (1 - `Molality, mol/kg c2 phase_2` * molweight2 / 1000) * (1 - `Solvent: Molality, mol/kg c1 phase_2` * molweight1 / 1000)
      ),
      !is.na(`Solvent: Molality, mol/kg c2 phase_2`) & !is.na(`Molality, mol/kg c1 phase_2`) ~ (
        (1 - `Molality, mol/kg c1 phase_2` * molweight1 / 1000) * (1 - `Solvent: Molality, mol/kg c2 phase_2` * molweight2 / 1000)
      ),
    )
  ) %>%
  mutate(
    mass_fraction_c1 = if_else(
      is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) & !is.na(mass_fraction_c3),
      1 - mass_fraction_c2 - mass_fraction_c3,
      mass_fraction_c1
    ),
    mass_fraction_c2 = if_else(
      !is.na(mass_fraction_c1) & is.na(mass_fraction_c2) & !is.na(mass_fraction_c3),
      1 - mass_fraction_c1 - mass_fraction_c3,
      mass_fraction_c2
    ),
    mass_fraction_c3 = if_else(
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) & is.na(mass_fraction_c3),
      1 - mass_fraction_c1 - mass_fraction_c2,
      mass_fraction_c3
    ),
  )

tmlframe %>%
  filter(!is.na(`Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`)) %>%
  select(where(~ !all(is.na(.x)))) %>%
  colnames()

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1 = case_when(
      !is.na(`Mole fraction c1 phase_2`) ~ `Mole fraction c1 phase_2`,
      !is.na(`Mole fraction c2 phase_2`) & !is.na(`Mole fraction c3 phase_2`) ~ 1 - `Mole fraction c2 phase_2` - `Mole fraction c3 phase_2`,
      !is.na(mass_fraction_c1) & !is.na(`Solvent: Mole fraction c2 phase_2`) ~ (
        mass_fraction_c1 / molweight1 / (
          mass_fraction_c1 / molweight1 + (1 - mass_fraction_c1) / (
            `Solvent: Mole fraction c2 phase_2` * molweight2 + (1 - `Solvent: Mole fraction c2 phase_2`) * molweight3
          )
        )
      ),
      !is.na(mass_fraction_c3) & !is.na(`Solvent: Mole fraction c2 phase_2`) ~ (
        (1 - `Solvent: Mole fraction c2 phase_2`) * (1 - mass_fraction_c3 / molweight3 / (
          mass_fraction_c3 / molweight3 + (1 - mass_fraction_c3) / (
            `Solvent: Mole fraction c2 phase_2` * molweight2 + (1 - `Solvent: Mole fraction c2 phase_2`) * molweight1
          )
        ))
      ),
      !is.na(mass_fraction_c3) & !is.na(`Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`) ~ (
        `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` / (
          1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`
        ) * (1 - mass_fraction_c3 / molweight3 / (
          mass_fraction_c3 / molweight3 + (1 - mass_fraction_c3) / (
            `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` / (
              1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`
            ) * molweight1 + 1 / (
              1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`
            ) * molweight2
          )
        ))
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c2 phase_2`) & !is.na(`Mole fraction c3 phase_2`) ~ (
        (1 - `Mole fraction c3 phase_2`) * 1 / (
          `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2` + 1
        )
      ),
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) & !is.na(mass_fraction_c3) ~ (
        mass_fraction_c1 / molweight1 / (mass_fraction_c1 / molweight1 + mass_fraction_c2 / molweight2 + mass_fraction_c3 / molweight3)
      ),
      !is.na(mass_fraction_c2) & !is.na(`Solvent: Mole fraction c1 phase_2`) ~ (
        `Solvent: Mole fraction c1 phase_2` * (1 - mass_fraction_c2 / molweight2 / (
          mass_fraction_c2 / molweight2 + (1 - mass_fraction_c2) / (
            `Solvent: Mole fraction c1 phase_2` * molweight1 + (1 - `Solvent: Mole fraction c1 phase_2`) * molweight3
          )
        ))
      ),
    ),
    mole_fraction_c2 = case_when(
      !is.na(`Mole fraction c2 phase_2`) ~ `Mole fraction c2 phase_2`,
      !is.na(`Mole fraction c1 phase_2`) & !is.na(`Mole fraction c3 phase_2`) ~ 1 - `Mole fraction c1 phase_2` - `Mole fraction c3 phase_2`,
      !is.na(`Solvent: Mole fraction c2 phase_2`) & !is.na(`Mole fraction c1 phase_2`) ~ (
        (1 - `Mole fraction c1 phase_2`) * `Solvent: Mole fraction c2 phase_2`
      ),
      !is.na(mass_fraction_c2) & !is.na(`Solvent: Mole fraction c1 phase_2`) ~ (
        mass_fraction_c2 / molweight2 / (
          mass_fraction_c2 / molweight2 + (1 - mass_fraction_c2) / (
            `Solvent: Mole fraction c1 phase_2` * molweight1 + (1 - `Solvent: Mole fraction c1 phase_2`) * molweight3
          )
        )
      ),
      !is.na(mass_fraction_c1) & !is.na(`Solvent: Mole fraction c2 phase_2`) ~ (
        `Solvent: Mole fraction c2 phase_2` * (1 - mass_fraction_c1 / molweight1 / (
          mass_fraction_c1 / molweight1 + (1 - mass_fraction_c1) / (
            `Solvent: Mole fraction c2 phase_2` * molweight2 + (1 - `Solvent: Mole fraction c2 phase_2`) * molweight3
          )
        )
        )
      ),
      !is.na(mass_fraction_c3) & !is.na(`Solvent: Mole fraction c2 phase_2`) ~ (
        `Solvent: Mole fraction c2 phase_2` * (1 - mass_fraction_c3 / molweight3 / (
          mass_fraction_c3 / molweight3 + (1 - mass_fraction_c3) / (
            `Solvent: Mole fraction c2 phase_2` * molweight2 + (1 - `Solvent: Mole fraction c2 phase_2`) * molweight1
          )
        ))
      ),
      !is.na(mass_fraction_c3) & !is.na(`Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`) ~ (
        1 / (
          1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`
        ) * (1 - mass_fraction_c3 / molweight3 / (
          mass_fraction_c3 / molweight3 + (1 - mass_fraction_c3) / (
            `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` / (
              1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`
            ) * molweight1 + 1 / (
              1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`
            ) * molweight2
          )
        ))
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c2 phase_2`) & !is.na(`Mole fraction c3 phase_2`) ~ (
        (1 - `Mole fraction c3 phase_2`) * `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2` / (
          `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2` + 1
        )
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`) & !is.na(`Mole fraction c1 phase_2`) ~ (
        (1 - `Mole fraction c1 phase_2`) * 1 / (
          1 + `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`
        )
      ),
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) & !is.na(mass_fraction_c3) ~ (
        mass_fraction_c2 / molweight2 / (mass_fraction_c1 / molweight1 + mass_fraction_c2 / molweight2 + mass_fraction_c3 / molweight3)
      ),
    ),
    mole_fraction_c3 = case_when(
      !is.na(`Mole fraction c3 phase_2`) ~ `Mole fraction c3 phase_2`,
      !is.na(`Mole fraction c1 phase_2`) & !is.na(`Mole fraction c2 phase_2`) ~ 1 - `Mole fraction c1 phase_2` - `Mole fraction c2 phase_2`,
      !is.na(`Solvent: Mole fraction c2 phase_2`) & !is.na(`Mole fraction c1 phase_2`) ~ (
        (1 - `Mole fraction c1 phase_2`) * (1 - `Solvent: Mole fraction c2 phase_2`)
      ),
      !is.na(mass_fraction_c1) & !is.na(`Solvent: Mole fraction c2 phase_2`) ~ (
        (1 - `Solvent: Mole fraction c2 phase_2`) * (1 - mass_fraction_c1 / molweight1 / (
          mass_fraction_c1 / molweight1 + (1 - mass_fraction_c1) / (
            `Solvent: Mole fraction c2 phase_2` * molweight2 + (1 - `Solvent: Mole fraction c2 phase_2`) * molweight3
          )
        )
        )
      ),
      !is.na(mass_fraction_c3) & !is.na(`Solvent: Mole fraction c2 phase_2`) ~ (
        mass_fraction_c3 / molweight3 / (
          mass_fraction_c3 / molweight3 + (1 - mass_fraction_c3) / (
            `Solvent: Mole fraction c2 phase_2` * molweight2 + (1 - `Solvent: Mole fraction c2 phase_2`) * molweight1
          )
        )
      ),
      !is.na(mass_fraction_c3) & !is.na(`Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`) ~ (
        mass_fraction_c3 / molweight3 / (
          mass_fraction_c3 / molweight3 + (1 - mass_fraction_c3) / (
            `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` / (
              1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`
            ) * molweight1 + 1 / (
              1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`
            ) * molweight2
          )
        )
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`) & !is.na(`Mole fraction c1 phase_2`) ~ (
        (1 - `Mole fraction c1 phase_2`) * `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2` / (
          1 + `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`
        )
      ),
      !is.na(mass_fraction_c1) & !is.na(mass_fraction_c2) & !is.na(mass_fraction_c3) ~ (
        mass_fraction_c3 / molweight3 / (mass_fraction_c1 / molweight1 + mass_fraction_c2 / molweight2 + mass_fraction_c3 / molweight3)
      ),
      !is.na(mass_fraction_c2) & !is.na(`Solvent: Mole fraction c1 phase_2`) ~ (
        (1 - `Solvent: Mole fraction c1 phase_2`) * (1 - mass_fraction_c2 / molweight2 / (
          mass_fraction_c2 / molweight2 + (1 - mass_fraction_c2) / (
            `Solvent: Mole fraction c1 phase_2` * molweight1 + (1 - `Solvent: Mole fraction c1 phase_2`) * molweight3
          )
        ))
      ),
    )
  ) %>%
  mutate(
    mole_fraction_c1 = if_else(
      is.na(mole_fraction_c1) & !is.na(mole_fraction_c2) & !is.na(mole_fraction_c3),
      1 - mole_fraction_c2 - mole_fraction_c3,
      mole_fraction_c1
    ),
    mole_fraction_c2 = if_else(
      !is.na(mole_fraction_c1) & is.na(mole_fraction_c2) & !is.na(mole_fraction_c3),
      1 - mole_fraction_c1 - mole_fraction_c3,
      mole_fraction_c2
    ),
    mole_fraction_c3 = if_else(
      !is.na(mole_fraction_c1) & !is.na(mole_fraction_c2) & is.na(mole_fraction_c3),
      1 - mole_fraction_c1 - mole_fraction_c2,
      mole_fraction_c3
    ),
  )

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1),
    !is.na(mole_fraction_c2),
    !is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tmlframe %>%
  filter(
    is.na(mole_fraction_c1) |
      is.na(mole_fraction_c2) |
      is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()


tmlframe <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1),
    !is.na(mole_fraction_c2),
    !is.na(mole_fraction_c3)
  ) %>%
  select(where(~ !all(is.na(.x))))

## Save

tmlframe %>%
  summary()

tmlframe %>%
  mutate(
    T_K = `Temperature, K phase_2`,
    st = m0_phase_2,
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "st_ternary.parquet"
  )

tml_saved <- read_parquet("st_ternary.parquet")
tml_saved %>% colnames()
