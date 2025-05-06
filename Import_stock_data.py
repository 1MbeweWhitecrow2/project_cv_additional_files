import pandas as pd
from django.core.management.base import BaseCommand
from stocks.models import Stock, StockData

class Command(BaseCommand):
    help = 'Import daily stock data from CSV file and update existing entries'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to CSV file')

    def handle(self, *args, **kwargs):
        csv_file = kwargs['csv_file']
        df = pd.read_csv(csv_file)

        # Replace NaN with 0
        df.fillna(0, inplace=True)

        for _, row in df.iterrows():
            stock, created = Stock.objects.update_or_create(
                ticker=row['ticker'],
                defaults={
                    'name': row['name'],
                    'sector': row['sector']
                }
            )

            if created:
                self.stdout.write(self.style.SUCCESS(f'New stock created: {row["ticker"]}'))

            # Parse date correctly
            data_date = pd.to_datetime(row['date']).date()

            # Create or update StockData
            stock_data, sd_created = StockData.objects.update_or_create(
                stock=stock,
                date=data_date,
                defaults={
                    'open_price': float(row['open_price']),
                    'high_price': float(row['high_price']),
                    'low_price': float(row['low_price']),
                    'close_price': float(row['close_price']),
                    'volume': int(row['volume']),
                    'eps': float(row['eps']),
                    'dividend_per_share': float(row['dividend_per_share']),
                    'repurchase_stock': float(row['repurchase_stock']),
                    'net_income': float(row['net_income']),
                    'normalized_income': float(row['normalized_income']),
                    'net_debt': float(row['net_debt']),
                    'total_assets': float(row['total_assets']),
                    'total_revenue': float(row['total_revenue'])
                }
            )

            if sd_created:
                self.stdout.write(self.style.SUCCESS(f'Data created for {row["ticker"]} on {data_date}'))
            else:
                self.stdout.write(self.style.WARNING(f'Data updated for {row["ticker"]} on {data_date}'))

        self.stdout.write(self.style.SUCCESS("✅ All data imported successfully!"))
