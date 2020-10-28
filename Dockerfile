FROM python:3.7

RUN mkdir ~/.streamlit
RUN mkdir src/
COPY . src/
WORKDIR src/
COPY config.toml ~/.streamlit/config.toml
COPY credentials.toml ~/.streamlit/credentials.toml
RUN pip3 install -r requirements.txt

EXPOSE 8501

CMD streamlit run main.py
