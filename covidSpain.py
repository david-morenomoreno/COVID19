from datetime import datetime
import csv
import requests
from elasticsearch import Elasticsearch


def formatCCAA(ccaa):
    if ccaa == "C. Valenciana":
        ccaa = "Comunidad Valenciana"
    elif ccaa == "Madrid":
        ccaa = "Comunidad de Madrid"
    elif ccaa == "Murcia":
        ccaa = "Región de Murcia"
    elif ccaa == "Baleares":
        ccaa = "Islas Baleares"

    return ccaa


# Spain urls
url_altas_spain = "https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_altas.csv"
url_casos_spain = "https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_casos.csv"
url_fallecidos_spain = "https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_fallecidos.csv"
url_uci_spain = "https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_uci.csv"


# CCAA
url_ccaa_cyl = "https://datosabiertos.jcyl.es/web/jcyl/risp/es/sector-publico/situacion-epidemiologica-coronavirus/1284940407131.csv"


def save_elasticsearch_es(index, result_data):

    es = Elasticsearch()

    es.indices.create(
        index=index,
        ignore=400  # ignore 400 already exists code
    )
    print(result_data)

    id_case = str(result_data['date'].timestamp()) + \
        '-'+result_data['CCAA']+'-'+result_data['type']
    es.index(index=index, id=id_case, body=result_data)


def get_data_csv_spain(base_url, index, case_type):
    '''
        :param base_url:
        :param index:
        :param type:

    '''

    with requests.get(base_url, stream=True) as r:
        lines = (line.decode('utf-8') for line in r.iter_lines())
        datasheets = list(csv.reader(lines))

        # Removing last lien with the Total
        del datasheets[-1]

        dateframe = datasheets[0][2:]
        for row in datasheets[1:]:
            ccaa = formatCCAA(row[1])

            result_data = {
                'CCAA': ccaa,
                'country': 'Spain',
            }

            previousData = 0
            infection_day_100 = 0

            for day, data in zip(dateframe, row[2:]):
                dataAux = int(data)
                data = int(data) - previousData
                previousData = dataAux

                if dataAux >= 100:
                    infection_day_100 += 1

                result_data.update(
                    date=datetime.strptime(day, "%Y-%m-%d"),
                    type=case_type,
                    count_case=int(data),
                    total_case=dataAux,
                    rate_100_infection=infection_day_100
                )
                save_elasticsearch_es(index, result_data)


if __name__ == '__main__':

    # COVID Spain
    index_name = 'covid_spain'
    get_data_csv_spain(url_altas_spain, index_name, 'recuperado')
    get_data_csv_spain(url_casos_spain, index_name, 'confirmado')
    get_data_csv_spain(url_fallecidos_spain, index_name, 'fallecido')
    get_data_csv_spain(url_uci_spain, index_name, 'uci')
