# -*- coding: utf-8 -*-

import logging

from tqdm import tqdm
from six import string_types
from django.conf import settings
from django.core.paginator import Paginator
from bulk_update.helper import bulk_update
from faker import Faker


logger = logging.getLogger(__name__)

try:
    faker = Faker(settings.LANGUAGE_CODE)
except AttributeError:
    faker = Faker()


class BaseAnonymizer:

    model = None
    attributes = None

    def __init__(self):
        if not self.model or not self.attributes:
            logger.info('ERROR: Your anonymizer is missing the model or attributes definition!')
            exit(1)

    def get_query_set(self):
        """
        You can override this in your Anonymizer.
        :return: QuerySet
        """
        return self.model.objects.all()

    def run(self, batch_size):
        instances = self.get_query_set()

        # When there are memory constraints is better to paginate the
        # processing of the instances
        paginator = Paginator(instances, batch_size)
        count_instances = 0
        count_fields = 0

        progress_bar = tqdm(desc="Processing", total=paginator.num_pages)
        for num_page in paginator.page_range:
            subinstances = paginator.page(num_page)
            instances_processed, count_subinstances, count_subfields = self._process_instances(subinstances)
            bulk_update(
                instances_processed,
                update_fields=[attrs[0] for attrs in self.attributes],
                batch_size=batch_size
            )

            count_instances += count_subinstances
            count_fields += count_subfields

            progress_bar.update(1)
        progress_bar.close()
        return len(self.attributes), count_instances, count_fields

    def _process_instances(self, instances):
        count_fields = 0
        count_instances = 0

        for model_instance in instances:
            for field_name, replacer in self.attributes:
                if callable(replacer):
                    replaced_value = self.get_allowed_value(replacer, model_instance, field_name)
                elif isinstance(replacer, string_types):
                    replaced_value = replacer
                else:
                    raise TypeError('Replacers need to be callables or Strings!')
                setattr(model_instance, field_name, replaced_value)
                count_fields += 1
            count_instances += 1

        return instances, count_instances, count_fields

    @staticmethod
    def get_allowed_value(replacer, model_instance, field_name):
        retval = replacer()
        max_length = model_instance._meta.get_field(field_name).max_length
        if max_length:
            retval = retval[:max_length]
        return retval
