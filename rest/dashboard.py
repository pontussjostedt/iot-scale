from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import plotly.graph_objs as go
import plotly.io as pio
from dataclasses import dataclass
import pandas as pd
import psycopg2
import env

TOPIC_HEADER = "topic"
TIMESTAMP_HEADER = "timestamp"
VALUE_HEADER = "value"

TOPIC_WEIGHT = "scale/data/weight"
TOPIC_HUMIDITY = "scale/data/humidity"
TOPIC_TEMPERATURE = "scale/data/temperature"

labels = {
    TOPIC_WEIGHT: "weight (g)",
    TOPIC_TEMPERATURE: "temperature (deg C)",
    TOPIC_HUMIDITY: "humidity (%)"
}

app = FastAPI()

DB_CONFIG = {
    'dbname': env.PG_DB,
    'user': env.PG_USERNAME,
    'password': env.PG_PASSWORD,
    'host': env.PG_HOST,
    'port': env.PG_PORT
}

@dataclass
class Data:
    weight: list[float]
    temp: list[float]
    hum: list[float]
    timestamp: list[str]

    @staticmethod
    def from_str(datas: list[str], timestamps: list[str]) -> 'Data':
        matrix: list[list[float]] = [list(map(float, data.split(" "))) for data in datas]
        weight, temp, hum = map(list, zip(*matrix))
        return Data(weight, temp, hum, timestamps)

    def plots(self):
        yield self.weight, "weight (g)"
        yield self.temp, "temperature (deg C)"
        yield self.hum, "humidity (%)"

def get_df(topics, timestamps, values) -> pd.DataFrame:
    df = pd.DataFrame({
        TOPIC_HEADER: pd.Series(topics, dtype="string"),
        TIMESTAMP_HEADER: pd.to_datetime(timestamps),
        VALUE_HEADER: pd.Series(values, dtype="float")}
    )
    
    df = df.drop_duplicates(TIMESTAMP_HEADER)
    df_pivot = df.pivot(index=TIMESTAMP_HEADER, columns=TOPIC_HEADER, values=VALUE_HEADER)
    df_filled = df_pivot.ffill()
    return df_filled

def get_data() -> pd.DataFrame:
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT topic, timestamp, msg FROM data")
            results = cur.fetchall()
            topics, timestamps, values = zip(*results)
            return get_df(topics, timestamps, values)

def add_experiment(name: str, about: str = ""):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO experiments(name, about) VALUES (%s, %s)", (name, about))
        conn.commit()

@app.get("/", response_class=HTMLResponse)
def plot_item():
    df = get_data()
    fig = go.Figure()
    timestamp = df.index
    for topic, name in labels.items():
        fig.add_trace(go.Scatter(x=timestamp, y=df[topic], mode='lines+markers',
                                 name=f'{name}', showlegend=True, line_shape='hv'))
    fig.update_layout(
        title="IOT data",
        xaxis_title="Timestamp",
        yaxis_title="Data",
        showlegend=True,
    )
    html = pio.to_html(fig, full_html=False)
    return HTMLResponse(content=html)


@app.get("/add_experiment/{experiment_name}")
def post_experiment(experiment_name: str):
    add_experiment(experiment_name)
    return 201
