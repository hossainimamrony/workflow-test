from django.shortcuts import redirect, render
from django.views import View
from .forms import VideoGenerationJobForm
from .services import VideoGenerationService


class WorkflowHomeView(View):
    template_name = "global_make_model_video/home.html"

    def get(self, request):
        form = VideoGenerationJobForm()
        return render(request, self.template_name, {"form": form})

    def post(self, request):
        form = VideoGenerationJobForm(request.POST)
        if form.is_valid():
            VideoGenerationService.queue_job(**form.cleaned_data)
            return redirect("global_make_model_video:home")
        return render(request, self.template_name, {"form": form})
