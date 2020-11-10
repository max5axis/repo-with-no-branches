/**aaaaaaaaaaaaaaaaaaaa
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
define(['N/record', 'N/search', 'N/format', 'N/email', 'N/render', 'N/runtime'],
	/**
	 * @param {record} record
	 * @param {search} search
	 */
	function(record, search, format, email, render, runtime) {
		var SEARCH_PROJECTMILESTONE = 'customsearch_projectmilestonetobeinvoice';
		var ITEM_NETSUITEPROJECT = '19';
		var PRICELEVEL_CUSTOM = '-1';
		var DEPARTMENT_SERVICES_DELIVERY = '2';
		var CLASS_NETSUITEPROJECT = '2';
		var EMPLOYEE_LP = '-5';
		var ITEM_DISCOUNT_NETSUITEPROJECT = '38';
		var ERROR_EMAIL_TEMPLATE = '21';
		var STANDARD_PRICE_LINE_NUM = 0;
		var EMPLOYEE_COMMUNICATIONS = 148;
		var EMPLOYEE_MAXIM = 205;
		/**
		 * Marks the beginning of the Map/Reduce process and generates input data.
		 *
		 * @typedef {Object} ObjectRef
		 * @property {number} id - Internal ID of the record instance
		 * @property {string} type - Record type id
		 *
		 * @return {Array|Object|Search|RecordRef} inputSummary
		 * @since 2015.1
		 */
		function getInputData() {
			return search.load({ //5 units
				id: SEARCH_PROJECTMILESTONE
			});
		}

		/**
		 * Group project milestones by Customers since we send 1 invoice by customer.
		 * @governance 0 units
		 * @param {MapSummary} context - Data collection containing the key/value pairs to process through the map stage
		 * @since 2015.1
		 */
		function map(context) {
			var result = JSON.parse(context.value);
			var resultValues = result.values;
			var customerId = resultValues['parent.CUSTRECORD_PROJECTMILESTONE_PROJECT'].value;
			var invoiceInfoObj = {
				quantity: resultValues.custrecord_projectmilestone_quotedeffort,
				rate: resultValues.custrecord_projectmilestone_hourlyrate,
				projectMilestoneId: result.id,
				projectName: resultValues['companyname.CUSTRECORD_PROJECTMILESTONE_PROJECT'],
				itemId: resultValues.custrecord_projectmilestone_item.value
			};
			context.write(customerId, invoiceInfoObj);
		}

		var addedToContextArray = [];
		/**
		 * Generate an invoice by customer with the Project Milestones and set the invoice on the Project Milestone
		 * @governance 30 units + 2 units per Project Milestone
		 * @param {ReduceSummary} context - Data collection containing the groups to process through the reduce stage
		 * @since 2015.1
		 */
		function reduce(context) {
			var lineItems = context.values;
			var invoiceId = createInvoice(context.key, lineItems); //30 units
			if (invoiceId != '')
				setInvoiceOnProjectMilestones(lineItems, invoiceId); //2 units per Project Milestone

			if(noCustomerItemPricingArray.length > 0 || noItemPricingArray.length > 0){

				for (var i = 0; i < noCustomerItemPricingArray.length; i++) {
					if(addedToContextArray.indexOf(JSON.stringify(noCustomerItemPricingArray[i])) !== -1)
						context.write('customer', noCustomerItemPricingArray[i]);
					addedToContextArray.push(JSON.stringify(noCustomerItemPricingArray[i]));
				}
				for (var i = 0; i < noItemPricingArray.length; i++) {
					if(addedToContextArray.indexOf(JSON.stringify(noItemPricingArray[i])) !== -1)
						context.write('item', noItemPricingArray[i]);
					addedToContextArray.push(JSON.stringify(noItemPricingArray[i]));
				}
			}
		}

		/**
		 * Executes when the summarize entry point is triggered and applies to the result set.
		 *
		 * @param {Summary} summary - Holds statistics regarding the execution of a map/reduce script
		 * @since 2015.1
		 */
		function summarize(summary) {
			var noCustomerPriceArray = [];
			var noItemPriceArray = [];
			summary.output.iterator().each(function(key, value) {
				if(key == 'customer'){
					noCustomerPriceArray.push(value);
					log.error({
						title: 'No customer item pricing found',
						details: value
					});
				}
				if(key == 'item')
				{
					noItemPriceArray.push(value);
					log.error({
						title: 'No standard item price found',
						details: value
					});
				}
				
				return true;
			});

			if(noCustomerPriceArray.length > 0 || noItemPriceArray.length > 0){
				log.error({
					title: 'Invoices creation failed.'
				});
				sendErrorEmail(EMPLOYEE_MAXIM,noCustomerPriceArray, noItemPriceArray);
			}else{
				log.audit({
					title: 'Invoices created.'
				});
			}

			// If an error was thrown during the input stage, log the error.

			if (summary.inputSummary.error)
			{
				log.error({
					title: 'Input Error',
					details: summary.inputSummary.error
				});
			}

			// For each error thrown during the map stage, log the error, the corresponding key,
			// and the execution number. The execution number indicates whether the error was
			// thrown during the the first attempt to process the key, or during a
			// subsequent attempt.

			summary.mapSummary.errors.iterator().each(function (key, error, executionNo){
				log.error({
					title: 'Map error for key: ' + key + ', execution no.  ' + executionNo,
					details: error
				});
				return true;
			});


			// For each error thrown during the reduce stage, log the error, the corresponding
			// key, and the execution number. The execution number indicates whether the error was
			// thrown during the the first attempt to process the key, or during a
			// subsequent attempt.

			summary.reduceSummary.errors.iterator().each(function (key, error, executionNo){
				log.error({
					   title: 'Reduce error for key: ' + key + ', execution no. ' + executionNo,
					   details: error
				});
				return true;
			 });
		}

		/**
		 * Create an invoice record for the customer
		 * @governance 30 units
		 * @param customerId (integer) : Customer's internalid
		 * @param lineItems (array) : Line items to be added on the invoice
		 * @returns invoiceId (integer) : Internal id of the invoice newly created
		 */
		function createInvoice(customerId, lineItems) {
			var invoiceRecord = record.create({ //10 units
				type: record.Type.INVOICE,
				isDynamic: true
			});

			//Set Body fields
			invoiceRecord.setValue({
				fieldId: 'entity',
				value: customerId
			});

			invoiceRecord.setValue({
				fieldId: 'department',
				value: DEPARTMENT_SERVICES_DELIVERY
			});

			invoiceRecord.setValue({
				fieldId: 'class',
				value: CLASS_NETSUITEPROJECT
			});

			invoiceRecord.setValue({
				fieldId: 'nextapprover',
				value: EMPLOYEE_LP
			});

			//Set date to be the last day of previous' month
			var lastDayPreviousMonth = addFullMonths(new Date(), -1, 32);
			var tranDate = format.format({
				value: lastDayPreviousMonth,
				type: format.Type.DATE
			});

			invoiceRecord.setValue({
				fieldId: 'trandate',
				value: format.parse({
					value: tranDate,
					type: format.Type.DATE
				})
			});

			//Set Line Items
			var numberOfLineItems = lineItems.length;
			//Loop through the array to parse and sort the line items to be displayed
			lineItems = parseAndSortLineItems(lineItems);
			for (var i = 0; i < numberOfLineItems; i++) {
				var lineObj = lineItems[i];
				
				var customerItemPriceObj = getCustomersItemPricing(customerId, lineObj.itemId);
				var itemStandardPrice = getItemPrice(lineObj.itemId, customerItemPriceObj.currency);
				
				if (!customerItemPriceObj.price || !itemStandardPrice)
					continue;
				
				var newLine = invoiceRecord.selectNewLine({
					sublistId: 'item'
				});

				invoiceRecord.setCurrentSublistValue({
					sublistId: 'item',
					fieldId: 'item',
					value: lineObj.itemId,
					ignoreFieldChange: false
				});

				invoiceRecord.setCurrentSublistValue({
					sublistId: 'item',
					fieldId: 'description',
					value: "Project: "+lineObj.projectName,
					ignoreFieldChange: false
				});


				invoiceRecord.setCurrentSublistValue({
					sublistId: 'item',
					fieldId: 'quantity',
					value: lineObj.quantity,
					ignoreFieldChange: false
				});
				
				invoiceRecord.setCurrentSublistValue({
					sublistId: 'item',
					fieldId: 'rate',
					value: customerItemPriceObj.price,
					ignoreFieldChange: false
				});

				invoiceRecord.setCurrentSublistValue({
					sublistId: 'item',
					fieldId: 'custcol_projectmilestone',
					value: lineObj.projectMilestoneId,
					ignoreFieldChange: false
				});

				invoiceRecord.commitLine({
					sublistId: 'item'
				});
				
				var discountRate = itemStandardPrice - customerItemPriceObj.price;
				
				if (discountRate > 0) {
					var newLine = invoiceRecord.selectNewLine({
						sublistId: 'item'
					});
					
					invoiceRecord.setCurrentSublistValue({
						sublistId: 'item',
						fieldId: 'item',
						value: ITEM_DISCOUNT_NETSUITEPROJECT,
						ignoreFieldChange: false
					});

					invoiceRecord.setCurrentSublistValue({
						sublistId: 'item',
						fieldId: 'price',
						value: PRICELEVEL_CUSTOM,
						ignoreFieldChange: false
					});
					
					invoiceRecord.setCurrentSublistValue({
						sublistId: 'item',
						fieldId: 'description',
						value: "Discount (" + lineObj.quantity + "*" + discountRate + ") on Project: " + lineObj.projectName,
						ignoreFieldChange: false
					});

					invoiceRecord.setCurrentSublistValue({
						sublistId: 'item',
						fieldId: 'quantity',
						value: PRICELEVEL_CUSTOM,
						ignoreFieldChange: false
					});
					
					invoiceRecord.setCurrentSublistValue({
						sublistId: 'item',
						fieldId: 'rate',
						value: discountRate*lineObj.quantity*-1,
						ignoreFieldChange: false
					});

					invoiceRecord.setCurrentSublistValue({
						sublistId: 'item',
						fieldId: 'custcol_projectmilestone',
						value: lineObj.projectMilestoneId,
						ignoreFieldChange: false
					});
					
					invoiceRecord.commitLine({
						sublistId: 'item'
					});
				}
			}
			
			if (noCustomerItemPricingArray.length == 0 && noItemPricingArray.length == 0){
				var invoiceId = invoiceRecord.save({ //20 units
					enableSourcing: true,
					ignoreMandatoryFields: true
				});

				return invoiceId;
			}
			
			return '';
		}

		/**
		 * Set the invoice newly created on the Project Milestones
		 * @governance 2 units per Project Milestone
		 * @param lineItems (array) : Array of line items on the invoice
		 * @param invoiceId (interger) : Invoice's internal id
		 * @returns nothing
		 */
		function setInvoiceOnProjectMilestones(lineItems, invoiceId) {
			var numberOfProjectMilestones = lineItems.length;
			for (var i = 0; i < numberOfProjectMilestones; i++) {
				var projectMilestoneId = JSON.parse(lineItems[i]).projectMilestoneId;
				var returnedId = record.submitFields({ //2 units
					type: 'customrecord_project_milestone',
					id: projectMilestoneId,
					values: {
						'custrecord_projectmilestone_invoiced': invoiceId
					}
				});
			}
		}
		/**
		 * Add months but prevent the new Date() function to increment the month when we are passing too many days possible for the month
		 * Example : if we do new Date(2020, 2, 30), since February 2020 only has 29 days and we pass 30, it will add 1 extra day to the date,
		 * @governance 0 units
		 * @param dateToAddMonth (date object) : Date to which we must add months
		 * @param monthsToAdd (integer) : Number of months to add to the date
		 * @param originalDayInMonth (integer) : Day of the month of the initial Start Date. We need to pass this because if we encounter a month with less days than the initial one, it will keep that lower date for all subsequent dates
		 * @returns finalDate (date object) : Date with the months added
		 */
		function addFullMonths(dateToAddMonth, monthsToAdd, originalDayInMonth) {
			var finalDate;
			var dayOfMonth = originalDayInMonth;
			var expectedMonth = new Date(dateToAddMonth.getFullYear(), dateToAddMonth.getMonth() + monthsToAdd, 1, 1).getMonth();
			var attemptedMonth = new Date(dateToAddMonth.getFullYear(), dateToAddMonth.getMonth() + monthsToAdd, dayOfMonth, 1).getMonth();
			if (expectedMonth != attemptedMonth) {
				do {
					dayOfMonth--;
					attemptedMonth = new Date(dateToAddMonth.getFullYear(), dateToAddMonth.getMonth() + monthsToAdd, dayOfMonth, 1).getMonth();
					finalDate = new Date(dateToAddMonth.getFullYear(), dateToAddMonth.getMonth() + monthsToAdd, dayOfMonth, 1);
				} while (expectedMonth != attemptedMonth);
			} else {
				finalDate = new Date(dateToAddMonth.getFullYear(), dateToAddMonth.getMonth() + monthsToAdd, dayOfMonth, 1);
			}

			return finalDate;
		}

		/**
		 * Loop through the array of stringified line items, parse them and sort by project then amount
		 * @governance 0 units
		 * @param arrayToTransform (array) : Array of stringified line items
		 * @returns arrayTransformed (array) : Array of line items properly parsed and sorted
		 */
		function parseAndSortLineItems(arrayToTransform) {
			var arrayTransformed = [];
			var numberOfLineItems = arrayToTransform.length;
			//Parse all elements first
			for (var i = 0; i < numberOfLineItems; i++) {
				arrayTransformed.push(JSON.parse(arrayToTransform[i]));
			}

			arrayTransformed.sort(function(a, b) {
				return a.projectName - b.projectName || (b.quantity * b.rate) - (a.quantity * a.rate);
			});

			return arrayTransformed;
		}
		
		var noCustomerItemPricingArray = [];
		
		function getCustomersItemPricing(customerId, itemId){
			
			var customerRecord = record.load({
				type: record.Type.CUSTOMER,
				id: customerId,
				isDynamic: true
			});
			
			var customerCurrency = customerRecord.getValue({
				fieldId: 'currency'
			});
			
			var customerPriceObj = {
				price: null,
				currency: customerCurrency
			};
			
			var count = customerRecord.getLineCount({sublistId: 'itempricing'});
			
			for(var i = 0; i < count; i++) {
				var customerItem = customerRecord.getSublistValue({
					sublistId: 'itempricing',
					fieldId: 'item',
					line: i
				});

				var currency = customerRecord.getSublistValue({
					sublistId: 'itempricing',
					fieldId: 'currency',
					line: i
				});
				
				if (customerItem == itemId && currency == customerCurrency) {		
							
					var price = customerRecord.getSublistValue({
						sublistId: 'itempricing',
						fieldId: 'price',
						line: i
					});

					if (typeof price == 'number')
						customerPriceObj.price = price;
				}
			}

			if (!customerPriceObj.price){
				var valueExists = false;
				for(var i = 0; i < noCustomerItemPricingArray.length; i++){
					if(noCustomerItemPricingArray[i].customer == customerId &&
						noCustomerItemPricingArray[i].item == itemId &&
						noCustomerItemPricingArray[i].currency == currency) {
							valueExists = true;
						}
				}
				if(!valueExists){
					noCustomerItemPricingArray.push({
						customer: customerId,
						item: itemId,
						currency: currency
					});
				}
			}
			
			return customerPriceObj;
		}
		var noItemPricingArray = [];
		
		function getItemPrice(itemId, currency){
			var itemRate = null;

			var itemRecord = record.load({
				type: record.Type.SERVICE_ITEM,
				id: itemId,
				isDynamic: true
			});
						
			var priceID = 'price' + currency;
			
			var rate = itemRecord.getSublistValue({
				sublistId : priceID,
				fieldId : 'price_1_',
				line : STANDARD_PRICE_LINE_NUM
			});

			if(typeof rate == 'number')
				itemRate = rate;

			if(!itemRate){
				var valueExists = false;
				for(var i = 0; i < noItemPricingArray.length; i++){
					if(noItemPricingArray[i].item == itemId &&
						noItemPricingArray[i].currency == currency) {
							valueExists = true;
						}
				}

				if(!valueExists){
					noItemPricingArray.push({
						item: itemId,
						currency: currency
					});
				}
			}
			
			return itemRate;
		}
		
		function sendErrorEmail(recipient, noCustomerPriceArray, noItemPriceArray) {
			var emailMerger = render.mergeEmail({
				templateId: ERROR_EMAIL_TEMPLATE,
				entity: {
					type: 'employee',
					id: recipient
				},
				recipient: null,
				supportCaseId: null,
				transactionId: null,
				custmRecord: null
			});

			var emailSubject = emailMerger.subject;
			var emailBody = emailMerger.body;
			
			var stringBuilderArray = [];
			stringBuilderArray.push("Customer records with no item pricing:")
			for (var i = 0; i < noCustomerPriceArray.length; i++) {
				var string = noCustomerPriceArray[i];
				
				stringBuilderArray.push(string);
			}
			stringBuilderArray.push("<br>Item records with no standard price:");
			for (var i = 0; i < noItemPriceArray.length; i++) {
				var string = noItemPriceArray[i];
				stringBuilderArray.push(string);
			}

			emailBody = emailBody.replace('!REPLACE!', stringBuilderArray.join("<br>"));
			
			email.send({
				author: EMPLOYEE_COMMUNICATIONS,
				recipients: recipient,
				subject: emailSubject,
				body: emailBody
			});
		}

		return {
			getInputData: getInputData,
			map: map,
			reduce: reduce,
			summarize: summarize
		};

	});