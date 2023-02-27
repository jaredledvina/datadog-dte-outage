FROM python:3 as builder
RUN pip install pipenv
WORKDIR /app
COPY Pipfile* /app
RUN mkdir /app/.venv
RUN pipenv install --deploy

FROM python:3-slim
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH
COPY main.py main.py

ENTRYPOINT [ "python", "main.py" ]
