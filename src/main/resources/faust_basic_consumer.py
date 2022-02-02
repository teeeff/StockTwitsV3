import faust

app = faust.App(
    'hello-world',
    broker='kafka://w01.itversity.com:9092;w02.itversity.com:9092;w03.itversity.com:9092',
    value_serializer='raw',
)

greetings_topic = app.topic('tdsktest')

@app.agent(greetings_topic)
async def greet(tdsktest):
    async for greeting in tdsktest:
        print(greeting)


"""
pip3 install --target=/home/itv000118/flask_work Flask

pip3 install --target=/home/itv000118/flask_work faust

export PATH=$PATH:/home/itv000118/flask_work



pip3 install --target=/home/itv000118/flask_work robinhood-aiokafka==0.5.0
pip3 install --target=/home/itv000118/flask_work robinhood-kafka-python==1.4.3


pip install  --target=/home/itv000118/flask_work python-snappy


pip3 install -U pytest-asyncio


python3 -m flask run --host=0.0.0.0 --port=1212



python3 -m flask run --host=0.0.0.0

export FLASK_APP=app.py
export FLASK_RUN_HOST=0.0.0.0
export FLASK_RUN_PORT=1212

python3 -m faust -A zz_sample_app worker -l info
"""