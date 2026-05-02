from django import forms
from .models import VideoGenerationJob


class VideoGenerationJobForm(forms.ModelForm):
    class Meta:
        model = VideoGenerationJob
        fields = ["title", "prompt"]
