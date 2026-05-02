from django import forms
from .models import ReelRenderJob


class ReelRenderJobForm(forms.ModelForm):
    class Meta:
        model = ReelRenderJob
        fields = ["source_name", "notes"]
