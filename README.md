# SellPulse: Product Ranking Dashboard

Run the dashboard in Docker; Python and the project dependencies are installed
inside the image, so they do not need to be installed on your machine.

## Start

Install and start Docker Desktop, then run this command from the project folder:

```sh
docker compose up --build
```

Open [http://localhost:8501](http://localhost:8501) in your browser. Stop the
dashboard with `Ctrl+C`; to run it in the background, use
`docker compose up --build -d` and stop it with `docker compose down`.

The container reads `results/top10_products.csv` from the project folder. The
results folder is mounted read-only, so you can update the CSV on your machine
and the dashboard can load the updated rankings without rebuilding the image.
The CSV must contain `product_name` and `units_sold` columns.