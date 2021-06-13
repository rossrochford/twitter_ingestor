from collections import defaultdict

from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Fieldset, ButtonHolder, Submit, HTML, Div, BaseInput
from django import forms
from django.template import Template, Context
from django.utils import html
import magic

from twitter.models import Tag, TwitterProfile


ACTION_CHOICES = [
    ('continue', 'Continue'), ('scrape-user-info', 'Scrape user-info')
]

WORK_TYPE_CHOICES = [
    'user_info',
    'user_timeline',
    'user_likes',
    'friend_ids',
    'follower_ids',
    'conversation_tweets',
]
WORK_TYPE_CHOICES = [(s, s) for s in WORK_TYPE_CHOICES]

PRIORITY_CHOICES = [(i, str(i)) for i in range(1, 4)]

SPREADSHEET_MIME_TYPES = (
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
)


class SendOneForm(forms.Form):

    id_strings = forms.CharField()
    work_type = forms.ChoiceField(
        label='work type',
        widget=forms.Select, choices=WORK_TYPE_CHOICES
    )

    def __init__(self, *args, **kwargs):
        # self.user = kwargs.pop('user')
        super().__init__(*args, **kwargs)

        self.helper = FormHelper()
        self.helper.layout = Layout(
            Fieldset('', 'id_strings', 'work_type'),
            ButtonHolder(
                Submit('submit', 'Next')
            )
        )

    def clean(self):

        work_type = self.cleaned_data['work_type']

        if work_type == 'conversation_tweets':
            ids = self.cleaned_data['id_strings'].split(',')
            ids = [i.strip() for i in ids]
            for id in ids:
                if not id.isdigit():
                    raise forms.ValidationError(f"{id} is not a numeric conversation id")

            self.cleaned_data['conversation_ids'] = ids
            return self.cleaned_data

        screen_names, user_ids = [], []
        for id in self.cleaned_data['id_strings'].split(','):
            id = id.strip().lower()
            if not id:
                continue
            if id.isdigit():
                user_ids.append(id)
            else:
                screen_names.append(id)

        profiles_by_sn, profiles_by_userid = {}, {}
        if screen_names:
            profiles_by_sn = {
                obj.screen_name: obj for obj in
                TwitterProfile.objects.filter(screen_name__in=screen_names)
            }
        if user_ids:
            profiles_by_userid = {
                obj.user_id: obj for obj in
                TwitterProfile.objects.filter(user_id__in=user_ids)
            }

        profiles = []
        for id in user_ids + screen_names:
            profile = profiles_by_userid.get(id) or profiles_by_sn.get(id)

            if work_type != 'user_info':
                if profile is None:
                    raise forms.ValidationError(f'profile {id} not found')
                if not profile.user_info:
                    raise forms.ValidationError(f'profile must have user_info before scraping: {work_type} ({id})')
                if profile.is_available is False:
                    raise forms.ValidationError(f'profile {profile.id} {id} is private or deleted')

            profiles.append(profile or id)

        self.cleaned_data['selected_profiles'] = profiles
        return self.cleaned_data


class ImportProfilesForm(forms.Form):

    def __init__(self, *args, **kwargs):
        # self.user = kwargs.pop('user')
        super(ImportProfilesForm, self).__init__(*args, **kwargs)

        self.helper = FormHelper()
        self.helper.layout = Layout(
            Fieldset('', 'spreadsheet_file'),
            ButtonHolder(
                Submit('submit', 'Submit')
            )
        )

    spreadsheet_file = forms.FileField()

    def clean_spreadsheet_file(self):
        in_memory_file = self.cleaned_data['spreadsheet_file']
        content = in_memory_file.read()
        mime = magic.from_buffer(content, mime=True)
        if mime not in SPREADSHEET_MIME_TYPES:
            raise forms.ValidationError(f'unexpected mime type: {mime}')
        self.cleaned_data['spreadsheet_file_content'] = content
        return self.cleaned_data['spreadsheet_file']


def _get_tags_by_category():
    tags_by_category = defaultdict(list)
    uncategorized = []
    for tag in Tag.objects.prefetch_related('categories').all():
        if tag.categories.count() > 0:
            for category in tag.categories.all():
                tags_by_category[category.slug].append(tag)
        else:
            uncategorized.append(tag)

    # add this last, so it appears at the end of the page
    tags_by_category['uncategorized'] = uncategorized

    return tags_by_category


class SelectTagsForm(forms.Form):

    def __init__(self, *args, **kwargs):
        # self.user = kwargs.pop('user')
        super().__init__(*args, **kwargs)

        tags_by_category = _get_tags_by_category()

        layout_args = []
        for cat_slug, tags in tags_by_category.items():
            choices = [(obj.slug, obj.slug) for obj in tags]
            field_name = f"tags_{cat_slug}"
            self.fields[field_name] = forms.MultipleChoiceField(
                 widget=forms.CheckboxSelectMultiple,
                label='', required=False, choices=choices
            )
            # fieldset_args = [cat_slug] + [t.slug for t in tags]
            layout_args.append(Fieldset(cat_slug, field_name))

        layout_args.append(
            ButtonHolder(
                Submit('submit', 'Next')
            )
        )
        self.helper = FormHelper()
        self.helper.layout = Layout(*layout_args)

    def clean(self):
        form_data = self.cleaned_data

        tag_slugs = set()
        for field, values in form_data.items():
            if field.startswith('tags_') and type(values) is list:
                for val in values:
                    tag_slugs.add(val)
        form_data['tag_slugs'] =list(tag_slugs)

        if len(tag_slugs) == 0:
            raise forms.ValidationError('no tags were selected')

        return form_data


class SubmitButtonCustom(BaseInput):

    input_type = "submit"

    def __init__(self, *args, **kwargs):
        # for valid values see: https://www.w3schools.com/bootstrap4/bootstrap_buttons.asp
        button_style = kwargs.get('button_style', 'primary')
        self.field_classes = f"btn btn-{button_style}"
        super().__init__(*args, **kwargs)


class SelectUserInfoActionForm(forms.Form):

    cancel = forms.Field(label="", required=False)
    continue_without_fetching = forms.Field(label="", required=False)
    fetch_userinfo = forms.Field(label="", required=False)

    def __init__(self, *args, **kwargs):
        num_with_user_info = kwargs.pop('num_with_user_info')
        super().__init__(*args, **kwargs)

        buttons = []
        if num_with_user_info > 0:
            buttons.append(
                SubmitButtonCustom('continue_without_fetching', 'Continue without fetching user-info', button_style='secondary')
            )
        else:
            buttons.append(
                SubmitButtonCustom('cancel', 'Cancel', button_style='secondary')
            )
        buttons.append(Submit('fetch_userinfo', 'Fetch missing user-info'))

        self.helper = FormHelper()
        self.helper.layout = Layout(ButtonHolder(*buttons))

    def clean(self):
        form_data = self.cleaned_data
        if not form_data:
            raise forms.ValidationError('missing button-field value')
        return form_data


class SelectScrapeTasksForm(forms.Form):

    scrape_user_timeline = forms.BooleanField(required=False)
    scrape_user_likes = forms.BooleanField(required=False)
    scrape_friend_ids = forms.BooleanField(required=False)
    scrape_follower_ids = forms.BooleanField(required=False)

    priority = forms.ChoiceField(
        choices=PRIORITY_CHOICES, initial=2, label='priority', required=False
    )
    limit = forms.IntegerField(label='limit', required=False)
    flush_queues = forms.BooleanField(initial=True, required=False)

    def __init__(self, *args, **kwargs):
        self.num_profiles = kwargs.pop('num_profiles')
        kwargs['initial']['limit'] = self.num_profiles
        super().__init__(*args, **kwargs)

        field_names = self.fields.keys()
        self.helper = FormHelper()
        self.helper.layout = Layout(
            Fieldset('', *field_names),
            ButtonHolder(
                Submit('submit', 'Submit')
            )
        )


class SelectScrapeTasksFormOLD(forms.Form):

    WORKLOAD_KEYS = [
        'user_timeline',
        'user_likes',
        'friend_ids',
        'follower_ids'
    ]

    SCRAPE_SECTION_TEMPLATE = """
        <div>
        <b>{{ key }}</b>: {{ num_profiles }} profiles
        </div>
    """

    def __init__(self, *args, **kwargs):
        self.num_profiles = kwargs.pop('num_profiles')
        for key in self.WORKLOAD_KEYS:
            kwargs['initial'][f'{key}_limit'] = self.num_profiles
        super().__init__(*args, **kwargs)

        layout_sections = []
        for key in self.WORKLOAD_KEYS:
            layout_sections.append(HTML('<hr/>'))
            layout_sections.append(
                self._create_worktype_summary_html(key)
            )
            layout_sections.append(
                Fieldset('', f'scrape_{key}', f'{key}_priority', f'{key}_limit')
            )
            layout_sections.append(HTML('<br/>'))

        layout_sections.append(HTML('<hr/>'))
        layout_sections.append(Fieldset('', 'flush_queues'))
        layout_sections.append(HTML('<br/>'))
        layout_sections.append(ButtonHolder(Submit('submit', 'Next')))

        self.helper = FormHelper()
        self.helper.layout = Layout(*layout_sections)

    def _create_worktype_summary_html(self, workload_key):
        context = {'num_profiles': self.num_profiles, 'key': workload_key}
        html_str = Template(self.SCRAPE_SECTION_TEMPLATE).render(Context(context))
        return HTML(html_str)

    scrape_user_timeline = forms.BooleanField(label='scrape', required=False)
    scrape_user_likes = forms.BooleanField(label='scrape', required=False)
    scrape_friend_ids = forms.BooleanField(label='scrape', required=False)
    scrape_follower_ids = forms.BooleanField(label='scrape', required=False)

    limit = forms.IntegerField(label='limit', required=False)
    priority = forms.ChoiceField(
        choices=PRIORITY_CHOICES, initial=2, label='priority', required=False
    )
    flush_queues = forms.BooleanField(initial=True, required=False)
