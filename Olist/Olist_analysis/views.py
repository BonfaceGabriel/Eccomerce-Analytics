import pandas as pd
from django.shortcuts import render
from rest_framework import status
from rest_framework.response import Response
from rest_framework import status, viewsets
from .analyze import revenue_growth, best_selling_categories, best_categories_by_revenue, analyze_order_weekday, analyze_order_hours, percentage_change_in_sales
# Create your views here.

class CountryViewSets(viewsets.ViewSet):
    def get_revenue_growth(self, request):
                try:
                    if request.method == 'POST':
                            date1 = request.data.get('date1')
                            date2 = request.data.get('date2')

                            result = revenue_growth(date1, date2)
                    return Response({"status": status.HTTP_200_OK, "message": "Succesfull", "payload":result}, content_type ='application/json')
                
                except Exception as e:
                    print(e)
                    return Response({"status": status.HTTP_501_NOT_IMPLEMENTED, 
                                    "message": "Error occured during implementation",
                                    "payload": None})
    
    def get_best_selling_categories(self, request):
                try:
                    if request.method == 'POST':
                            data = request.data['number']
                            result = best_selling_categories(data)
                    return Response({"status": status.HTTP_200_OK, "message": "Succesfull", "payload":result}, content_type ='application/json')
                
                except Exception as e:
                    print(e)
                    return Response({"status": status.HTTP_501_NOT_IMPLEMENTED, 
                                    "message": "Error occured during implementation",
                                    "payload": None})
                
    def get_best_categories_by_revenue(self, request):
                try:
                    if request.method == 'POST':
                            data = request.data['number']
                            result = best_categories_by_revenue(data)
                    return Response({"status": status.HTTP_200_OK, "message": "Succesfull", "payload":result}, content_type ='application/json')
                
                except Exception as e:
                    print(e)
                    return Response({"status": status.HTTP_501_NOT_IMPLEMENTED, 
                                    "message": "Error occured during implementation",
                                    "payload": None})
    
    def get_order_hours(self, request):
                try:
                    if request.method == 'GET':
                            result = analyze_order_hours()
                    return Response({"status": status.HTTP_200_OK, "message": "Succesfull", "payload":result}, content_type ='application/json')
                
                except Exception as e:
                    print(e)
                    return Response({"status": status.HTTP_501_NOT_IMPLEMENTED, 
                                    "message": "Error occured during implementation",
                                    "payload": None})
                
    def get_orders_by_dayofweek(self, request):
                try:
                    if request.method == 'GET':
                            result = analyze_order_weekday()
                    return Response({"status": status.HTTP_200_OK, "message": "Succesfull", "payload":result}, content_type ='application/json')
                
                except Exception as e:
                    print(e)
                    return Response({"status": status.HTTP_501_NOT_IMPLEMENTED, 
                                    "message": "Error occured during implementation",
                                    "payload": None})
    
    def get_percentage_change_in_sales(self, request):
                try:
                    if request.method == 'POST':
                            date1 = request.data.get('date1')
                            date2 = request.data.get('date2')

                            result = percentage_change_in_sales(date1, date2)
                    return Response({"status": status.HTTP_200_OK, "message": "Succesfull", "payload":result}, content_type ='application/json')
                
                except Exception as e:
                    print(e)
                    return Response({"status": status.HTTP_501_NOT_IMPLEMENTED, 
                                    "message": "Error occured during implementation",
                                    "payload": None})

    
    
                
    