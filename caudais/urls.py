from django.urls import path
from . import views


app_name = 'caudais'
urlpatterns = [
    path('dashboard/', views.dashboard, name='dashboard'),
    path('dashboard_comparison/', views.dashboard_comparison, name='dashboard_comparison'),
    path('exportar_excel/', views.exportar_excel, name='exportar_excel'),
    path('exportar_pdf/', views.exportar_pdf, name='exportar_pdf'),
    path('obter_series_por_ponto/', views.obter_series_por_ponto, name='obter_series_por_ponto'),
    path('get_anos_por_serie/', views.get_anos_por_serie, name='get_anos_por_serie'),
    path('upload/', views.upload_novo_ponto, name='upload_novo_ponto'),
    path('upload/nova-serie/', views.upload_nova_serie, name='upload_nova_serie'),
    path('upload/adicionar-valores/', views.upload_adicionar_valores, name='upload_adicionar_valores'),

]
