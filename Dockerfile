FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY distill_guard/ ./distill_guard/

EXPOSE 8080

# 默认影子模式: 只记日志不处置。开处置把 DG_SHADOW 置 0。
ENV DG_SHADOW=1 \
    DG_HOST=0.0.0.0 \
    DG_PORT=8080

CMD ["python", "-m", "distill_guard"]
