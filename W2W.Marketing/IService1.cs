using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;

namespace W2W.Marketing
{
    [ServiceContract]
#if DEBUG

#else
    [XmlSerializerFormat]
#endif

    public interface IService1
    {
        [OperationContract]
        uint BuyCamulative(
            uint companyId,
            uint marketingId,
            uint partnerId,
            decimal sum,
            DateTime date,
            string user);

        [OperationContract]
        uint BuyInvestment(
            uint companyId,
            uint camulativeMarketingId,
            uint partnerId,
            decimal sum,
            bool isProlonged,
            bool isCreateNewPlace,
            DateTime date,
            string user);


        [OperationContract]
        void PayInvestPercents(DateTime date, uint companyId, string user);

        [OperationContract]
        bool IsAllowCreateNewPlace(uint marketingId, uint partnerId);

        [OperationContract]
        uint CreateTechPlace(
            uint companyId,
            uint marketingId,
            DateTime date,
            string user);

        //[OperationContract]
        //void SetNullBalance(decimal sum);

        //[OperationContract]
        //void Test();

        [OperationContract]
        int UpdateBalances();

        [OperationContract]
        void ConfirmWithdrawal(uint id_object, string wallet, string tag, string comment, string user);

        [OperationContract]
        void CancelWithdrawal(uint id_object, string comment, string user);

        [OperationContract]
        void UnionInvestments(
            uint companyId,
            uint partnerId,
            decimal sum,
            uint[] investments,
            DateTime date,
            string user);

        [OperationContract]
        void TerminateInvestment(
            uint companyId,
            uint partnerId,
            uint investmentId, 
            decimal returnSum,
            DateTime date, 
            string user);
    }


}
