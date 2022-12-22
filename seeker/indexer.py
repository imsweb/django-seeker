import logging

from django.db.models import signals

from .registry import model_documents
from .utils import delete, index


logger = logging.getLogger(__name__)


class ModelIndexer(object):
    """
    Class that automatically indexes any new or deleted mapped model objects.
    """

    def connect_signal_handlers(self):
        """
        Connects save and delete signal handler for mapped models. Also checks each ModelIndex for any additional signal handling that may be needed. 
        """

        for model_class, document_classes in model_documents.items():
            signals.post_save.connect(self.handle_save, sender=model_class)
            signals.post_delete.connect(self.handle_delete, sender=model_class)

            for document_class in document_classes:
                document_class.connect_additional_signal_handlers(self)

    def disconnect_signal_handlers(self):
        for model_class, document_classes in model_documents.items():
            signals.post_save.disconnect(self.handle_save, sender=model_class)
            signals.post_delete.disconnect(self.handle_delete, sender=model_class)

            for document_class in document_classes:
                document_class.disconnect_additional_signal_handlers(self)

    def handle_save(self, sender, instance, **kwargs):
        try:
            index(instance)
        except Exception as e:
            logger.exception('Error indexing %s instance: %s. Exception raised: %s', sender, instance, e)

    def handle_delete(self, sender, instance, **kwargs):
        try:
            delete(instance)
        except Exception as e:
            logger.exception('Error deleting %s instance: %s. Exception raised: %s', sender, instance, e)

    def handle_m2m_changed(self, sender, instance, action, **kwargs):
        if action in ('post_add', 'post_remove', 'post_clear'):
            try:
                index(instance)
            except Exception as e:
                logger.exception('Error indexing many to many change %s instance: %s. Exception raised: %s', sender, instance, e)
