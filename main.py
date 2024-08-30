import requests
from bs4 import BeautifulSoup
import re
import json
from confluent_kafka import Producer

# Configurações do Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'client.id': 'carros-scraper'
}

# Criando o producer Kafka
producer = Producer(conf)

# Função de callback para reportar a entrega
def delivery_report(err, msg):
    if err is not None:
        print(f'Erro ao enviar mensagem: {err}')
    else:
        print(f'Mensagem enviada: {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

# URL base do site que você deseja fazer scraping
url_base = 'https://www.icarros.com.br/ache/listaanuncios.jsp?pag={}&ord=35&sop=esc_4.1_-rai_50.1_-'

# Definindo o User-Agent
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Número de páginas para fazer scraping
num_pages = 2

for page in range(1, num_pages + 1):
    # Atualizando a URL com o número da página
    url = url_base.format(page)

    # Fazendo a requisição
    response = requests.get(url, headers=headers)

    # Verificando o status da requisição
    if response.status_code == 200:
        # Parseando o conteúdo HTML
        soup = BeautifulSoup(response.content, 'html.parser')

        # Encontrando todos os cartões de oferta de carros
        ofertas = soup.find_all('div', class_='offer-card__header')

        for oferta in ofertas:
            try:
                # Extraindo o atributo onclick que contém o item_name
                onclick_attr = oferta.find('a', class_='offer-card__title-container')['onclick']

                # Usando regex para capturar o valor do item_name
                match = re.search(r"item_name:\s*'([^']+)'", onclick_attr)

                if match:
                    nome_carro = match.group(1)

                    # Extraindo o preço correspondente
                    preco_carro = oferta.find_next('div', class_='offer-card__price-container').find('p').text.strip()

                    # Encontrando a primeira imagem relacionada ao carro
                    img_tag = oferta.find_previous('div', class_='swiper-wrapper').find('img', class_='offer-card__image')
                    src_imagem = img_tag['src'] if img_tag else 'Imagem não encontrada'

                    # Criando o dicionário com os dados do carro
                    dados_carro = {
                        'model': nome_carro,
                        'price': preco_carro,
                        'image': src_imagem
                    }

                    # Enviando os dados para o Kafka
                    producer.produce(
                        topic='teste', 
                        key=nome_carro, 
                        value=json.dumps(dados_carro), 
                        callback=delivery_report
                    )
            except Exception as e:
                print(f'Erro ao processar oferta: {e}')
    else:
        print(f'Erro ao acessar a página {page}: {response.status_code}')

# Aguarda o envio de todas as mensagens
producer.flush()


# from confluent_kafka import Producer

# # Configuração do producer
# config = {
#     'bootstrap.servers': 'localhost:9092',  # Substitua pelo seu servidor Kafka
#     'client.id': 'python-producer'
# }

# # Criação do Producer
# producer = Producer(config)

# # Callback opcional para confirmação de envio
# def delivery_report(err, msg):
#     if err is not None:
#         print(f'Erro ao enviar mensagem: {err}')
#     else:
#         print(f'Mensagem enviada para {msg.topic()} [{msg.partition()}]')

# # Função para enviar mensagens
# def send_message(topic, key, value):
#     producer.produce(topic, key=key, value=value, callback=delivery_report)
#     producer.flush()

# # Enviar uma mensagem
# send_message('teste', key='minha-chave1', value='CHUPA CURINTHIA')

