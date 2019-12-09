
CODE_CHECKPOINT_MAPPING = {
    'ConsolidatedFunding':'BRE',
    'WithdrawalAttempt':'Withdrawal',
    'FundingPOS':'POS',
    'HoldsAssessment':'Fulfillment',
    'SimilityTxn':'Simility',
    'EvalConsumerCreditUS':'ConsumerCreditUS',
    'EvalConsumerCreditUK':'ConsumerCreditUK',
    'EvalBestCustomerSupportAction':'Intent'
}

EVE_EVENT_MAPPING = {'IAcctCreditCardEvent':'CreditCardCreated', \
            'IAcctSellerDisputeEvent':'DisputeNotification',\
            'IAcctSellerFaEvent':'UserFundsAvailabilityEvent',\
            'IDisputeCaseSender':'DisputeLifecycle',\
            'ITxnEventFunding':'Withdrawal',\
            'ITxnSenderPaymentAttempt':'PaymentAttempt',
            'ITxnEventSender':'PaymentDebit',
            'PaymentEvaluateAttemptEvent': 'BTPaymentAttempt',
            'VenmoAddFundingInstrument':'VenmoAddFI',
            'VenmoRemoveFundingInstrument':'VenmoRemoveFI',
            'BankCompletionPostProcess':'BankCompletion',
            'PaymentDebitPostProcess':'PaymentDebit',
            'PaymentAttemptVO':'PaymentAttempt',
            'PartnerTxnAttemptEvent':'PartnerTxnAttempt',
            'WithdrawalAttempt':'RiskWithdrawalAttemptDecision',
            'FptiMuseEvent':'MUSE_EDGE',
            'EdgePostPaidTabPayoffCompletedWorkflow':'PostpaidTabPayoffCompleted',
            'RefundMessage':'Refund',
            'ReversalMessage':'Reversal'
             }


VC_CP_MAPPING = {'WITHDRAWAL':'Withdrawal', \
            'LOGIN':'Login',\
            'DCAUTH':'DCAuth' }