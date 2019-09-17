from utils import utils
from entities.watermark import Watermark
from entities.var import Var
from entities.radd import RADD
from entities.model import Model
from entities.edge_container import EdgeContainer
import ssl
import json
import urllib2
from utils.graph_db import graph

class EVEUpdator(object):
    """
        update from eve API before finally migrate dashboard to EVEBuilder
    """
    def __init__(self):
        self.EVENT_LIST = {"PaymentDebit" , "Withdrawal" , "PaymentAttempt" , "UnilateralPaymentCompeletion" , "BankCompletion" , "DisputeNotification" , "CreditCardCreated" , "UserEmailChanged" , "YodleeBankAccountTransactionData" , "BankAccountAdded" , "NewAccountCreated" , "UserPhoneChanged" , "CCAuthResult" , "DecisionResult" , "LoginSucceeded" , "LoginFailed" , "LoginChanged" , "AddressChanged" , "BankAccountChanged" , "PaymentCredit" , "DecisionAttempt" , "Reversal" , "Refund" , "DepositComplete" , "CreditCardAddRejected" , "CreditCardChanged" , "PrimaryBankAccountChanged" , "UserAccountChanged" , "PaymentCompletion" , "FIWalletInstrumentAdded" , "LimitationAdded" , "LimitationLifted" , "LimitationUpdated" , "LimitationEscalated" , "DisputeLifecycle" , "LoginPartial" , "WithdrawalReversal" , "UserDataChanged" , "FidoLifeCycleEvents" , "FIWalletInstrumentRemoved" , "AccessInstrumentValidationAttemptedMessage" , "RiskPartnerTxnAttemptEvent" , "IDICaseEvent" , "BAEventsMessage" , "RuleCheckpointEvent" , "UserIdentityVerification" , "DepositAttemptRejectedRequest" , "LoginAttemptEvaluation" , "POSISO" , "MadmenBREVariableTrack" , "UserFundsAvailabilityEvent" , "BRECheckpointResult" , "LexaContact" , "VenmoCacheOutTransactionDataLakeTable" , "VenmoCashOut" , "VenmoAddFI" , "OnboradingGCCheckpointResult" , "RiskWithdrawalAttemptDecision" , "LoginCheckpointResult" , "BTPaymentAttempt" , "VenmoRemoveFI" , "PaymentAttemptVO" , "VenmoProfileChange" , "PaymentDebitNewDO" , "DisputeLifecycleNewDO" , "RDAConsolidatedDO" , "BraintreeTxnDatalakeTable" , "PartnerDeposit" , "WithdrawalCheckpointResult" , "PaymentAttemptNewDO" , "MUSE_EDGE" , "RiskAccountTokenCardEvents" , "FPTI_LOGIN" , "NewAccountCreatedNewDO"}
    
    def run_eve_updator(self):
        for event in self.EVENT_LIST:
            self.get_event_raw_dependency_from_eve(event, event)
    def parse_eve_keys():
        pass
        

    def get_event_key_dependency_from_eve(self, event, table_name):
            pass
        
    def get_event_raw_dependency_from_eve(self, event, table_name):
        """
        weekly fetch from evebuilder eventkey -[:ATTRIBUTE_OF] - raw relationship
                                and  set raw edge's updator, type, filter, target, etc

        """
        URL = 'http://grds.paypalinc.com/evebuilder/api/metadata/raw_variables?searchType=by_definition&searchData={event},,,,,,&searchSimilar=false'

        MAP_EVENT_VAR ="match(v:Var{{ name: '{var}'}})   \
                        merge(e:EventKey{{name: '{eventkey}' }}) \
                        merge (v)-[:ATTRIBUTE_OF] ->(e) "

        SET_EVE_PROPERTY = "match(v:Var{{ name: '{var}'}}) set v.filter = '{filter}', v.eve_type= '{type}', \
                        v.eve_key='{key}', v.target=\"{target}\", v.function='{function}' "
        
        opener = urllib2.build_opener()
        opener.addheaders.append(('Cookie', 'edge_builder=s%3Av79eqgLsj0XwgxJYigllG8sYRn2JZqdx.xyqUIdp7X7uROP5j6gK2i7UppsDfjr21hYbJyCUCKd0'))
        html = opener.open(URL.format(event=event))
        json_file = json.loads(html.read())
        

        #with open("decisionresult.json", "r") as read_file:
        #    json_file = json.load(read_file)


        var_list = json_file['data']
        for var in var_list:
            json_keys =  var.keys()
            for updator in var['updators']:
                try:
                    if updator['rollupMessages'] == event:
                        print (SET_EVE_PROPERTY.format(var=var['name'], filter= updator['filter'] , \
                        type = var['type'], key =updator['key'], target=updator['target'], function = updator['func'] ))
                        graph.cypher.execute(SET_EVE_PROPERTY.format(var=var['name'], filter= updator['filter'] , \
                        type = var['type'], key =updator['key'], target=updator['target'], function = updator['func'] ))
                        print MAP_EVENT_VAR.format(eventkey=table_name+'.'+updator['key'], var=var['name'])
                        graph.cypher.execute(MAP_EVENT_VAR.format(eventkey=table_name+'.'+updator['key'], var=var['name']))
                except:
                    pass
EVEUpdator().run_eve_updator()