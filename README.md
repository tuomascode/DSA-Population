### How to Set Up Environment

```
poetry install
poetry env activate
```

See [here](https://python-poetry.org/docs/) for how to install poetry.

As a fallback you may use:
```
python -m venv env
env\Scripts\activate
pip install -r requirements.txt
```

When you are done working (either workflow) use:
`deactivate`

#### Database Attributions
Population Data: United Nations, Department of Economic and Social Affairs, Population Division (2024). World Population Prospects 2024: Methodology of the United Nations population estimates and projections (UN DESA/POP/2024/DC/NO. 10).

Country Location Data (countries.json): [dr5hn/countries-states-cities-database](https://github.com/dr5hn/countries-states-cities-database), which is made available under the Open Database License (ODbL). The full license can be found at http://opendatacommons.org/licenses/odbl/1-0/.


#### Licensing
This database is made available under the **Open Database License: [http://opendatacommons.org/licenses/odbl/1-0/](http://opendatacommons.org/licenses/odbl/1-0/)**. Any rights in individual contents of the database are licensed as follows:

* **Country Location Data:** Licensed under the **Open Database License (ODbL-1.0)**.
* **Population Data:** Licensed under the **Creative Commons Attribution 3.0 IGO (CC BY 3.0 IGO)**.
