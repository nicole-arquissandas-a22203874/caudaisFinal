from django import forms
from django.contrib.auth.models import User

class UserRegistrationForm(forms.ModelForm):
    password1 = forms.CharField(label='Password', widget=forms.PasswordInput)
    password2 = forms.CharField(label='Confirm Password', widget=forms.PasswordInput)

    class Meta:
        model = User
        fields = ['username', 'email']

    def clean_password2(self):
        password1 = self.cleaned_data.get('password1')
        password2 = self.cleaned_data.get('password2')
        if password1 and password2 and password1 != password2:
            raise forms.ValidationError("Passwords don't match")
        return password2

    def save(self, commit=True):
        user = super().save(commit=False)
        user.set_password(self.cleaned_data['password1'])
        if commit:
            user.save()
        return user

class PasswordResetForm(forms.Form):
    email = forms.EmailField(label="Email", max_length=254)

class SetPasswordForm(forms.Form):
    password_nova = forms.CharField(label="Nova password", widget=forms.PasswordInput)
    password_confirmar = forms.CharField(label="Confirme a sua password", widget=forms.PasswordInput)

    def clean(self):
        cleaned_data = super().clean()
        password_nova = cleaned_data.get("password_nova")
        password_confirmar = cleaned_data.get("password_confirmar")

        if password_nova and password_confirmar and password_nova != password_confirmar:
            raise forms.ValidationError("As passwords não coincidem")

        return cleaned_data

