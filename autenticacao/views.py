from django.http import HttpResponse
from django.contrib.auth import login, logout, get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AuthenticationForm
from django.urls import reverse
from django.utils.http import urlsafe_base64_encode, urlsafe_base64_decode
from django.utils.encoding import force_bytes
from django.contrib.auth.tokens import default_token_generator
from django.shortcuts import render, redirect
from django.contrib import messages
from .forms import PasswordResetForm, SetPasswordForm, UserRegistrationForm
import urllib.request
import urllib.parse
import base64
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

User = get_user_model()


def send_mailgun_email(subject, body, to_email):
    url = f"https://api.eu.mailgun.net/v3/{settings.MAILGUN_DOMAIN}/messages"
    auth = base64.b64encode(f"api:{settings.MAILGUN_API_KEY}".encode()).decode()
    data = urllib.parse.urlencode({
        "from": f"CaudaisApp <mailgun@{settings.MAILGUN_DOMAIN}>",
        "to": to_email,
        "subject": subject,
        "html": body
    }).encode()

    request = urllib.request.Request(url, data=data)
    request.add_header("Authorization", f"Basic {auth}")

    try:
        with urllib.request.urlopen(request) as response:
            return response.read()
    except urllib.error.URLError as e:
        logger.error(f"Error sending email: {e}")
        return None


@login_required
def users_page(request):
    return redirect('caudais:dashboard')


def registo(request):
    if request.method == 'POST':
        form = UserRegistrationForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, 'Conta criada com sucesso! Pode agora fazer login.')
            return redirect('autenticacao:login')
    else:
        form = UserRegistrationForm()
    return render(request, 'autenticacao/registar.html', {'form': form})

def password_reset_request(request):
    if request.method == "POST":
        form = PasswordResetForm(request.POST)
        if form.is_valid():
            email = form.cleaned_data['email']
            user = User.objects.filter(email=email).first()
            if user:
                uid = urlsafe_base64_encode(force_bytes(user.pk))
                token = default_token_generator.make_token(user)
                reset_url = request.build_absolute_uri(
                    reverse('autenticacao:password_reset_confirm', kwargs={'uidb64': uid, 'token': token})
                )
                email_subject = 'Pedido de Redefinição de Senha'
                email_body = f"""
                <html>
                <body>
                    <p>Olá {
                    user.username}
                    ,</p>
                    <p>Recebemos um pedido para redefinir a sua senha. Clique no link abaixo para redefinir a sua senha:</p>
                    <p><a href="
                    {reset_url}
                    ">Redefinir Senha</a></p>
                    <p>Se não solicitou a redefinição de senha, por favor ignore este email.</p>
                    <p>Obrigado,</p>
                    <p>CaudaisApp</p>
                </body>
                </html>
                    """
                response = send_mailgun_email(email_subject, email_body, email)
                if response is None:
                    return HttpResponse('Error sending email.')
            return redirect('autenticacao:password_reset_done')
    else:
        form = PasswordResetForm()
    return render(request, 'autenticacao/password_reset.html', {'form': form})


def password_reset_done(request):
    return render(request, 'autenticacao/password_reset_done.html')


def password_reset_confirm(request, uidb64=None, token=None):
    if request.method == "POST":
        form = SetPasswordForm(request.POST)
        if form.is_valid():
            uid = urlsafe_base64_decode(uidb64).decode()
            user = User.objects.get(pk=uid)
            if default_token_generator.check_token(user, token):
                user.set_password(form.cleaned_data['password_nova'])
                user.save()
                return redirect('autenticacao:password_reset_complete')
    else:
        form = SetPasswordForm()
    return render(request, 'autenticacao/password_reset_confirm.html', {'form': form, 'uidb64': uidb64, 'token': token})


def password_reset_complete(request):
    return render(request, 'autenticacao/password_reset_complete.html')




def custom_login(request):
    if request.method == 'POST':
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            user = form.get_user()
            login(request, user)
            next_url = request.POST.get('next') or reverse('caudais:dashboard')
            return redirect(next_url)
        else:
            return render(request, 'autenticacao/login.html', {
                'form': form,
                'next': request.POST.get('next', ''),
                'mensagem': 'Credenciais inválidas'
            })
    else:
        if request.user.is_authenticated:
            return redirect('caudais:dashboard')
        else:
            form = AuthenticationForm()
            return render(request, 'autenticacao/login.html', {
                'form': form,
                'next': request.GET.get('next', '')
            })


def custom_logout(request):
    logout(request)
    messages.success(request, 'Logout realizado com sucesso!')
    return redirect('autenticacao:login')