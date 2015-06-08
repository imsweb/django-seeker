(function($) {
	// Handle facet ranges
    $('.range-pair').change(function() {
        var q1 = $(this).val().replace(/\D/, ''); // Remove any non-digit characters
        var q2 = $(this).siblings('.range-pair').val().replace(/\D/, ''); // Remove any non-digit characters
        var hiddenInput = $(this).siblings('.range-hidden');
        var separator = $(this).siblings('.range-separator').text();
        if( q1 || q2 ) {
        	hiddenInput.val($(this).hasClass('range-min') ? q1 + ' ' + separator + ' ' + q2 : q2 + ' ' + separator + ' ' + q1);
        	hiddenInput.prop('disabled', false);
        }
        else {
        	hiddenInput.val('');
        	hiddenInput.prop('disabled', true);
        }
    });
}(jQuery));
