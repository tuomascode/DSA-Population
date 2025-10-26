# GDP per Capita Prediction

This project aims to predict changes in GDP per capita using social and demographic features.

## The Aim and Uses

The aim of this project and website is to provide both policymakers and investors with
a rough idea of how different social and demographic factors impact GDP per capita.

We collected and analyzed data from 154 countries covering the period 1960–2023.
Our dataset includes the following features:

1. Country name
2. Country alpha-2 code
3. Year
4. Population
5. GDP per capita
6. Life expectancy
7. Net migration
8. Proportion of population using the internet
9. WDI Human Capital Index
10. WDI School Enrollment Index
11. Proportion of urban population
12. Infant mortality rate

## Results of Analysis

Our main finding was that changes in these features did affect GDP per capita,
but primarily in the long term. This makes intuitive sense — improving school enrollment
or human capital should not yield short-term results. We also observed the somewhat
obvious fact that, in the short term, the best predictor of GDP per capita is its own
previous values.

This provides two kinds of insights. For policymakers, the key takeaway is that
improvements made today can be expected to bear fruit only in the long term.
Policy design should therefore adopt a long-term perspective on when to expect changes.
Short-term fluctuations in GDP per capita can be quite random and noisy, often driven by
external factors such as financial crises or other global events. Chasing short-term
improvements also presents the epistemic challenge of disentangling which policies
produced which effects amid such randomness.

For investors, the insight is straightforward: if you are pursuing long-term investments,
you should look for countries that have recently improved in key indicators but have not yet
realized corresponding GDP per capita growth. Conversely, investors seeking short-term
gains should not rely on these features as predictors of near-term growth.

## The Product

We offer a website built around our collected data and predictive models. It allows users
to quickly and easily visualize and explore data, compare countries, download datasets,
train and use models, and predict GDP per capita by selecting feature values.
Essentially, the website serves as a playground for policymakers, long-term investors,
and other interested parties to visualize and compare economic data across countries and years.
