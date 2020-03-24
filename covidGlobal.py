from datetime import datetime
import csv
import requests
from elasticsearch import Elasticsearch
from multiprocessing import Process


# Global urls
url_confirmed = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"
url_deaths = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"
url_recovered = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"


def save_elasticsearch_global(index, result_data):
    es = Elasticsearch()

    mapping = {
        "mappings": {
            "properties": {
                "location": {
                    "type": "geo_point"
                }
            }
        }
    }

    es.indices.create(
        index=index,
        ignore=400,  # ignore 400 already exists code
        body=mapping
    )
    print(result_data)

    id_case = str(result_data['date'].timestamp(
    )) + '-'+result_data['state']+'-'+result_data['country']+'-'+result_data['type']
    es.index(index=index, id=id_case, body=result_data)


def get_data_csv_global(base_url, index, case_type):
    '''
        :param base_url:
        :param index:
        :param type:
    '''

    with requests.get(base_url, stream=True) as r:
        lines = (line.decode('utf-8') for line in r.iter_lines())
        datasheets = list(csv.reader(lines))

        dateframe = datasheets[0][4:]

        for row in datasheets[1:]:
            result_data = {
                'state': row[0],
                'country': row[1],
                "location": {
                    "lat": row[2],
                    "lon": row[3]
                }
            }

            previousData = 0
            infection_day_100 = 0

            for day, data in zip(dateframe, row[4:]):
                dataAux = int(data)
                data = int(data) - previousData
                previousData = dataAux

                if dataAux >= 100:
                    infection_day_100 += 1

                result_data.update(
                    date=datetime.strptime(day, "%m/%d/%y"),
                    type=case_type,
                    count_case=int(data),
                    total_case=dataAux,
                    rate_100_infection=infection_day_100
                )

                proc = Process(target=save_elasticsearch_global,
                               args=(index, result_data,))
                proc.start()


if __name__ == '__main__':

    # # COVID Global
    index_name = 'covid_global'
    get_data_csv_global(url_recovered, index_name, 'recuperado')
    get_data_csv_global(url_confirmed, index_name, 'confirmado')
    get_data_csv_global(url_deaths, index_name, 'fallecido')
