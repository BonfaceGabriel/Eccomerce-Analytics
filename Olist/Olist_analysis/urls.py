from django.urls import path
from .views import CountryViewSets


urlpatterns = [
    path('revenue-growth/', CountryViewSets.as_view({'post': 'get_revenue_growth'}), name='revenue'),
    path('best-selling-categories/', CountryViewSets.as_view({'post': 'get_best_selling_categories'}), name='best-sales-categories'),
    path('best_categories_by_revenue/', CountryViewSets.as_view({'post': 'get_best_categories_by_revenue'}), name='best-revenue-categories'),
    path('sales_by_hour/', CountryViewSets.as_view({'get': 'get_order_hours'}), name='order-by-hour-of-day'),
    path('sales_by_weekday/', CountryViewSets.as_view({'get': 'get_orders_by_dayofweek'}), name='order-by-dayofweek'),
    path('percentage_sales/', CountryViewSets.as_view({'post': 'get_percentage_change_in_sales'}), name='percentage-sales')

]