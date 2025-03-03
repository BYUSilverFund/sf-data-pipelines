from pipelines.covariance_matrix import covariance_matrix_constructor
from datetime import date
import pipelines.data_access_layer as dal
from pipelines.enums import Interval

start_date = end_date = date(2024, 1, 2)

univ = dal.load_universe(
    interval=Interval.DAILY,
    start_date=start_date,
    end_date=end_date,
    quiet=False
)

cov = covariance_matrix_constructor(
    date_=start_date,
    barrids=univ['barrid'].unique().to_list()
)

print(cov)