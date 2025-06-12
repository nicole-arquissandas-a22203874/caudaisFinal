from django.urls import path
from . import views

app_name = 'autenticacao'
urlpatterns = [
    path('', views.users_page, name='dashboard'),
    path('dashboard/', views.users_page, name='dashboard'),
    path('login/', views.custom_login, name='login'),
    path('logout/', views.custom_logout, name='logout'),
    path('registo/', views.registo, name='registar'),
    path('password_reset/', views.password_reset_request, name='password_reset'),
    path('password_reset/done/', views.password_reset_done, name='password_reset_done'),
    path('reset/<uidb64>/<token>/', views.password_reset_confirm, name='password_reset_confirm'),
    path('reset/done/', views.password_reset_complete, name='password_reset_complete'),
]
