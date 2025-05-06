from django.core.management.base import BaseCommand
from stocks.models import Stock
import pandas as pd
from django.db import transaction

class Command(BaseCommand):
    help = "Update stock names and sectors from CSV"

    def handle(self, *args, **kwargs):
        df = pd.read_csv("ticker_name_sector.csv")

        with transaction.atomic():
            for _, row in df.iterrows():
                ticker = row["ticker"]
                name = row["name"]
                sector = row["sector"]

                try:
                    stock = Stock.objects.get(ticker=ticker)
                    updated = False

                    if stock.name in ["0", "", None]:
                        stock.name = name
                        updated = True

                    if stock.sector in ["0", "", None]:
                        stock.sector = sector
                        updated = True

                    if updated:
                        stock.save()
                        self.stdout.write(self.style.SUCCESS(f"✅ Updated {ticker}: Name='{name}', Sector='{sector}'"))
                    else:
                        self.stdout.write(f"ℹ️ No update needed for {ticker}")

                except Stock.DoesNotExist:
                    self.stdout.write(self.style.WARNING(f"⚠️ Stock with ticker {ticker} not found."))

        self.stdout.write(self.style.SUCCESS("✅ Database update completed."))
