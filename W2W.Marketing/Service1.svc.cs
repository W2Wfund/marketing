using Binance.Net;
using Binance.Net.Objects;
using CryptoExchange.Net.Authentication;
using KBTech.Lib;
using KBTech.Lib.Client;
using KBTech.Lib.Client.Payment;
using KBTech.Lib.Query;
using KBTech.Lib.Query.Enums;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;
using W2W.ModelKBT;
using W2W.ModelKBT.Entities;
using MimeKit;
using MailKit;

namespace W2W.Marketing
{
    public class PlacePos
    {
        public MarketingPlace Parent { get; set; }
        public int Pos { get; set; }
    }

    public class PartnerInvestData
    {
        public NewInvestProgram Program { get; set; }
        public decimal BalanceInvestments { get; set; }
        public decimal MaxLimit { get; set; }
        public decimal AllPaymentsSum { get; set; }
        public bool isCanPay { get; set; }
        public decimal DayBinaryPaymentsSum { get; set; }
        public decimal MonthBinaryPaymentsSum { get; set; }
        public decimal AllBinaryPaymentsSum { get; set; }
        public decimal AllReferalPaymentsSum { get; set; }
        public decimal DoubleInvestSum { get; set;}

        public Partner partner { get; set; }
    }


    // ПРИМЕЧАНИЕ. Команду "Переименовать" в меню "Рефакторинг" можно использовать для одновременного изменения имени класса "Service1" в коде, SVC-файле и файле конфигурации.
    // ПРИМЕЧАНИЕ. Чтобы запустить клиент проверки WCF для тестирования службы, выберите элементы Service1.svc или Service1.svc.cs в обозревателе решений и начните отладку.
    public class Service1 : IService1
    {
        static object lockObj = new object();
        private static string mainPath;

        public void UnionInvestments(
            uint companyId,
            uint partnerId,
            decimal sum,
            uint[] investments,
            DateTime date,
            string user)
        {
            lock (lockObj)
            {
                IDataService ds = new KbtDataService();
                var partner = ds.GetPartner(partnerId);

                if (sum < 0)
                    throw new Exception("Неккоректно указана сумма");

                // проверка договоров
                List<Investment> items = new List<Investment>();
                foreach (var id in investments)
                {
                    var investment = ds.GetInvestment(id);
                    if (investment == null)
                        throw new Exception("Договор не найден");

                    // проверяем статус договора
                    if (investment.Status == "Завершен")
                        throw new Exception("Договор уже завершен");

                    // сверяем partnerId с investment.PartnerId
                    if (investment.PartnerId != partnerId)
                        throw new Exception("Договор не соответствует партнеру");

                    items.Add(investment);
                }

                // проверка остатка
                var allsum = items.Sum(x => x.Sum ?? 0) + sum;
                var reminder = GetForInvestReminderSum(
                    sum: allsum,
                    balance: partner.BalanceInner ?? 0);
                if (reminder <= 0)
                    throw new Exception("Недостаточно средств на лицевом счете!");

                // Поиск инвест программы
                InvestProgram program = ds.GetInvestProgram(reminder);
                if (program == null)
                    throw new Exception("Не найдена программа удовлетворяющая условиям");

                // Получить сумму под шаг
                var steppedsum = GetSteppedSum(reminder, program.Step);

                // расторжение договоров
                foreach (var item in items)
                {
                    TerminateInvestment(
                        companyId: companyId,
                        partnerId: partnerId,
                        investmentId: item.id_object,
                        returnSum: item.Sum ?? 0,
                        date: date,
                        user: user,
                        isNotClose: false);
                }

                // создание нового договора
                // создание договора
                var documentId = this.CreateDocumnt(
                    ds: ds,
                    companyId: companyId,
                    partnerId: partnerId,
                    programId: program.id_object,
                    percent: program.YearPercent,
                    date: date.AddDays(1),
                    sum: steppedsum,
                    isProlonged: false,
                    user: user);

                // списание с лицевого счета
                ds.PayPayment(ds.CreateInnerTransfer(
                    accountName: "Остаток.ВнутреннийСчет",
                    direction: TransferDirection.Output,
                    companyId: companyId,
                    partnerId: partnerId,
                    documentId: documentId,
                    article: $"Покупка инвест. пакета",
                    date: date,
                    sum: steppedsum,
                    paymentMethod: PaymentMethod.Inner,
                    comment: $"Покупка инвест. пакета {program.ObjectName}",
                    documentType: null,
                    user: user), date);

                // 10% от sum - спонсору
                var refawardsum = GetRefAward(
                    oldinvestmentssum: items.Sum(x => x.Sum ?? 0),
                    newinvestmentsum: steppedsum);
                if (refawardsum > 0)
                {
                    ds.PayPayment(ds.CreateInnerTransfer(
                        accountName: "Остаток.Вознаграждения",
                        direction: TransferDirection.Input,
                        companyId: companyId,
                        partnerId: partner.InviterId.Value,
                        documentId: partnerId,
                        article: "Реферальное вознаграждение",
                        date: date,
                        sum: refawardsum * 0.1m,
                        paymentMethod: PaymentMethod.Inner,
                        comment: $"Реферальное вознаграждение за {partner.ObjectName}",
                        documentType: null,
                        user: user), date);
                }
            }
        }

        // tested: 20.11.18 20:36
        public decimal GetForInvestReminderSum(
            decimal sum,
            decimal balance)
        {
            // если balance больше 0, то sum
            // если balance < 0, но sum превышает долг, то sum - долг
            // если balance < 0, но sum < долг, то 0
            if (balance >= 0)
                return sum;
            else if (balance + sum > 0)
                return balance + sum;
            else
                return 0;
        }

        // tested: 20.11.18 20:52
        public decimal GetSteppedSum(
            decimal sum,
            decimal step)
        {
            // Если balance отрицательный
            return Math.Floor(sum / step) * step;
        }

        // tested: 20.11.18 21:01
        public decimal GetRefAward(
            decimal oldinvestmentssum,
            decimal newinvestmentsum)
        {
            if (newinvestmentsum > oldinvestmentssum)
                return newinvestmentsum - oldinvestmentssum;
            else
                return 0;
        }

        public void TerminateInvestment(
            uint companyId,
            uint partnerId,
            uint investmentId, decimal returnSum,
            DateTime date, string user,bool? isNotClose)
        {
            lock (lockObj)
            {
                IDataService ds = new KbtDataService();
                var investment = ds.GetInvestment(investmentId);
                if (investment == null)
                    throw new Exception("Договор не найден");

                // проверяем статус договора
                if (investment.Status == "Завершен")
                    throw new Exception("Договор уже завершен");

                // сверяем partnerId с investment.PartnerId
                if (investment.PartnerId != partnerId)
                    throw new Exception("Договор не соответствует партнеру");

                // удаляем неоплаченные платежи
                var expected = ds.GetExpectedPayments(investmentId, DateTime.MaxValue);
                foreach (var item in expected)
                {
                    if (item.PaymentStatus == "Не оплачен")
                    {
                        ds.RemovePayment(item.id_object);
                    }

                }



                // списываем с инвест счета investment.Sum
                ds.PayPayment(ds.CreateInnerTransfer(
                    accountName: "Остаток.ИнвестиционныйСчет",
                    direction: TransferDirection.Output,
                    companyId: companyId,
                    partnerId: partnerId,
                    documentId: investmentId,
                    article: $"Закрытие инвест программы",
                    date: date,
                    sum: investment.Sum ?? 0,
                    paymentMethod: PaymentMethod.Inner,
                    comment: $"Закрытие инвест программы {investment.ProgramName}",
                    documentType: null,
                    user: user), date);


                // начисляем на лицевой счет returnSum
                if (returnSum > 0)
                {
                    ds.PayPayment(ds.CreateInnerTransfer(
                       accountName: "Остаток.ВнутреннийСчет",
                       direction: TransferDirection.Input,
                       companyId: companyId,
                       partnerId: partnerId,
                       documentId: investmentId,
                       article: $"Закрытие инвест программы",
                       date: date,
                       sum: returnSum,
                       paymentMethod: PaymentMethod.Inner,
                       comment: $"Закрытие инвест программы {investment.ProgramName}",
                       documentType: null,
                       user: user), date);
                }

                // меняем статус на Завершен
                // меняем дату изменения
                ds.SetInvestmentStatus(
                    investmentId: investmentId,
                    status: /*isNotClose == true ? "Закрыт" :*/ "Завершен",
                    changeDate: date);
            }
        }


        // old not use
        public uint BuyCamulative(
            uint companyId,
            uint marketingId,
            uint partnerId,
            decimal sum,
            DateTime date,
            string user)
        {
            return 0;
            /*lock (lockObj)
            {
                IDataService ds = new KbtDataService();

                var partner = ds.GetPartner(partnerId);
                if ((partner.BalanceInner ?? 0) < sum)
                    throw new Exception("Недостаточно средств на лицевом счете!");

                uint placeId = 0;

                #region Create Place

                // Если мест нет, то 
                if (ds.GetPlaceCount(marketingId, partnerId) == 0)
                {
                    placeId = this.CreateCamulativeProgramPlace(
                        ds: ds,
                        companyId: companyId,
                        marketingId: marketingId,
                        partnerId: partnerId,
                        objectName: partner.ObjectName,
                        alias: partner.Login,
                        sum: sum,
                        date: date,
                        user: user,
                        pos: new PlacePos());
                }
                else
                {
                    // Повторная оплата по месту
                    placeId = this.RePayPlace(
                        ds: ds,
                        partnerId: partnerId,
                        companyId: companyId,
                        marketingId: marketingId,
                        date: date,
                        user: user);
                }
                #endregion

                // Списание с лицевого счета
                ds.PayPayment(ds.CreateInnerTransfer(
                    accountName: "Остаток.ВнутреннийСчет",
                    direction: TransferDirection.Output,
                    companyId: companyId,
                    partnerId: partnerId,
                    documentId: placeId,
                    article: "Открытие накопит программы",
                    date: date,
                    sum: sum,
                    paymentMethod: PaymentMethod.Inner,
                    comment: "Открытие накопит программы Camulative",
                    documentType: null,
                    user: user), date);

                // Агентское вознаграждение
                ds.PayPayment(ds.CreateInnerTransfer(
                  accountName: "Остаток.Вознаграждения",
                  direction: TransferDirection.Input,
                  companyId: companyId,
                  partnerId: partner.InviterId.Value,
                  documentId: placeId,
                  article: "Реферальное вознаграждение",
                  date: date,
                  sum: 50,
                  paymentMethod: PaymentMethod.Inner,
                  comment: $"Реферальное вознаграждение за {partner.ObjectName}",
                  documentType: null,
                  user: user), date);

                ds.UpdatePartnerActivity(partnerId, true);

                return placeId;
            }*/

        }


        public uint BuyInvestment(
            uint companyId,
            uint camulativeMarketingId,
            uint partnerId,
            decimal sum,
            bool isProlonged,
            bool isCreateNewPlace,
            DateTime date,
            string user)
        {
            lock (lockObj)
            {
                IDataService ds = new KbtDataService();

                var partner = ds.GetPartner(partnerId);

               /* BinanceClient.SetDefaultOptions(new BinanceClientOptions()
                {
                    ApiCredentials = new ApiCredentials(
                        key: ConfigurationManager.AppSettings["binance.key"],
                        secret: ConfigurationManager.AppSettings["binance.secret"])                    
                });
                using (var client = new BinanceClient())
                {
                    var comission = client.GetAssetDetails();
                    decimal binance_comission = 0;

                    if (comission.Success)
                    {
                        //binance_comission = comission.Data[request.Currency].WithdrawFee;
                    }
                }*/


                    // Поиск инвест программы
                    NewInvestProgram program = ds.GetNewInvestProgram(sum);
                if (program == null)
                    throw new Exception("Не найдена программа удовлетворяющая условиям");

                if ((partner.BalanceInner ?? 0) < sum)
                    throw new Exception("Недостаточно средств на лицевом счете!");

                bool isSandBox = false;
                var rootPlaces = ds.GetPlaces(93, partner.id_parent);
                uint rootPlaceId = 0;
                if (rootPlaces.Count() > 0)
                {
                    rootPlaceId = rootPlaces.OrderBy(x => x.hash.Length).First().id_object;
                    var places = ds.GetStructure(93, rootPlaceId);
                    isSandBox = places.Count() >= 3;
                }

                //var inviter = ds.GetPartner(partner.InviterId.Value);

                #region Создание мест
                int countPlaces = ds.GetPlaceCount(camulativeMarketingId, partnerId);
                // Если уровень ниже 4 то создаем место, иначе в песочницу
                if (countPlaces == 0 && !isSandBox)//(ds.GetPlaceCount(camulativeMarketingId, partnerId) == 0 || isCreateNewPlace)
                {
                    var newPlaceId = this.CreateCamulativeProgramPlace(
                        ds: ds,
                        companyId: companyId,
                        marketingId: camulativeMarketingId,
                        partnerId: partnerId,
                        objectName: partner.ObjectName,
                        alias: partner.Login,
                        sum: sum,
                        date: date,
                        user: user,
                        pos: new PlacePos());

                    if (newPlaceId > 0)
                    {
                        partner.IsHasMarketPlace = true;
                    }
                }

                /*if(countPlaces > 0)
                {
                    // Повторная оплата по месту
                    this.RePayPlace(
                        ds: ds,
                        partnerId: partnerId,
                        companyId: companyId,
                        marketingId: camulativeMarketingId,
                        date: date,
                        user: user);
                }*/

                #endregion
                // создание договора
                var documentId = this.CreateDocumnt(
                    ds: ds,
                    companyId: companyId,
                    partnerId: partnerId,
                    programId: program.id_object,
                    percent: program.YearPercent,
                    date: date,
                    sum: sum,
                    isProlonged: isProlonged,
                    user: user);

                //добавляем сумму плечей всем вышестоящим, если есть место
                if (documentId > 0 && partner.IsHasMarketPlace == true)
                {
                    var structure = ds.GetStructure(93);
                    if (structure.Count() > 0)
                    {
                        var PartnerPlace = structure.SingleOrDefault(x => x.PartnerId == partnerId);
                        if (PartnerPlace != null)
                        {
                            UpdateParentInvestShoulderSum(PartnerPlace, structure, sum);
                        }
                    }
                }

                // списание с лицевого счета
                ds.PayPayment(ds.CreateInnerTransfer(
                    accountName: "Остаток.ВнутреннийСчет",
                    direction: TransferDirection.Output,
                    companyId: companyId,
                    partnerId: partnerId,
                    documentId: documentId,
                    article: $"Открытие инвест программы",
                    date: date,
                    sum: sum,
                    paymentMethod: PaymentMethod.Inner,
                    comment: $"Открытие инвест программы {program.ObjectName}",
                    documentType: null,
                    user: user), date);


                /* System.Collections.ArrayList list = new System.Collections.ArrayList();
                 list.AddRange(new uint[] { 76, 86, 83, 292, 27396, 89, 14195, 274, 84, 24802, 293, 88, 90, 91, 275, 276, 277, 279, 280 });
                 if (!list.Contains(partnerId))
                 {*/

                var parentPartner = ds.GetPartner(partner.id_parent);
                if (parentPartner != null)
                {
                    var investments = ds.GetInvestments(parentPartner.id_object, "Активен");
                    uint referalLimit = 0;
                    //decimal sumInvestments = 0;
                    uint maxLimit = 0;
                    decimal allPaymentsSum = 0;
                    string refLog = $"--{DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss")}\r\n—-Фио/логин { ShortName(parentPartner)}/{parentPartner.Login} \r\n";
                    bool isCloseInvest = false;
                    int countInvestments = investments.Count();
                    uint investProgramId = 0;
                    decimal doubleInvestSum = 0;
                   /* if (countInvestments == 0)
                    {
                        investments = ds.GetInvestments(parentPartner.id_object, "Закрыт");
                        countInvestments = investments.Count();
                        isCloseInvest = countInvestments > 0;
                    }*/

                    if (countInvestments > 0)
                    {
                        /*foreach (var item in investments)
                        {
                            sumInvestments += item.Sum ?? 0;
                        }*/
                        
                        var bigInvestment = investments.OrderByDescending(x => x.Sum).First();
                        if (bigInvestment.ProgramId > 0)
                        {
                            var investProgram = ds.GetInvestProgram(bigInvestment.ProgramId);
                            investProgramId = investProgram.id_object;
                            if (investProgram.ReferalLimit > 0)
                            {
                                referalLimit = (uint)investProgram.ReferalLimit;
                            }
                            if (investProgram.MaxLimit > 0)
                            {
                                maxLimit = (uint)investProgram.MaxLimit;
                            }
                            var oneInvest = investments.OrderBy(x => x.StartDate).First();
                            //Сумма первой инвестиции * 2
                            doubleInvestSum = (decimal)oneInvest.Sum * 2;
                            decimal AllBinaryPayments = CalculateAllBinaryPayments(parentPartner.id_object, ds, oneInvest.StartDate ?? DateTime.Now);

                            allPaymentsSum = CalculateAllPayments(parentPartner.id_object, ds, oneInvest.StartDate ?? DateTime.Now);
                            allPaymentsSum += AllBinaryPayments;
                        }
                    }

                    //bool flagIsAddPay = true;
                    //decimal sumReferals = 0;
                    decimal sumForPay = sum * 0.1m;
                    decimal extraPaySum = 0;

                    
                    if (countInvestments > 0)
                    {
                        refLog += $"--Реферальное вознаграждение\r\n---Лимит с личного приглашенного:{referalLimit.ToString()}\r\n---Лимит доходности:{maxLimit.ToString()}\r\n---Сумма всех начислений:{allPaymentsSum.ToString()};";


                        if (referalLimit > 0)
                        {
                            if (sumForPay > referalLimit)
                            {
                                extraPaySum += sumForPay - referalLimit;
                                sumForPay = referalLimit;
                            }
                        }
                        refLog += $"\r\n---проверка лимита с личного приглашенного\r\n---Сумма компании:{extraPaySum.ToString()},Сумма партнеру:{sumForPay.ToString()};";

                        if (maxLimit > 0 && maxLimit <= (sumForPay + allPaymentsSum))
                        {
                            extraPaySum += sumForPay + allPaymentsSum - maxLimit;
                            sumForPay -= sumForPay + allPaymentsSum - maxLimit;

                            /*if (isCloseInvest)
                            {
                                ds.SetInvestmentStatus(
                                investmentId: investProgramId,
                                status: "Завершен",
                                changeDate: date);
                            }
                            else
                            {*/
                            isCloseInvest = true;
                            closeInvestment(companyId, parentPartner.id_object, 0, DateTime.Now, user, ds);
                            refLog += "Закрытие пакета\r\n";
                            // }
                        }
                        refLog += $"\r\n---проверка лимита доходности.\r\n---Сумма компании:{extraPaySum.ToString()},Сумма партнеру:{sumForPay.ToString()};";
                        //decimal doubleInvestSum = (parentPartner.BalanceInvestments ?? 0) * 2;
                        // проверка двухкратной выплаты суммы
                        if (allPaymentsSum + sumForPay >= doubleInvestSum)
                        {

                            //extraPaySum += allPaymentsSum + sumForPay - doubleInvestSum;
                            //sumForPay -= allPaymentsSum + sumForPay - doubleInvestSum;
                            /*if (allPaymentsSum < doubleInvestSum)
                            {
                                sumForPay = doubleInvestSum - allPaymentsSum + sumForPay;
                            }*/

                            
                            if (!isCloseInvest)
                            {
                                closeInvestment(companyId, parentPartner.id_object, 0, DateTime.Now, user, ds);
                                refLog += "Закрытие пакета\r\n";
                            }
                        }
                        refLog += $"---проверка двухкратной выплаты суммы. Всего:{(allPaymentsSum + sumForPay).ToString()} Двукратная сумма:{doubleInvestSum.ToString()};Сумма партнеру: {sumForPay.ToString()}; Сумма компании:{extraPaySum.ToString()};\r\n";
                    }//нет инвестиций
                    else
                    {
                        extraPaySum = sumForPay;
                    }


                    if (extraPaySum > 0)
                    {                        
                        ds.PayPayment(ds.CreateInnerTransfer(
                                accountName: "Остаток.Вознаграждения",
                                direction: TransferDirection.Input,
                                companyId: companyId,
                                partnerId: companyId,
                                documentId: partnerId,
                                article: "Реферальное вознаграждение экстра",
                                date: date,
                                sum: extraPaySum,
                                paymentMethod: PaymentMethod.Inner,
                                comment: $"Реферальное вознаграждение за {partner.ObjectName}",
                                documentType: null,
                                user: user), date);
                    }

                    if (sumForPay > 0)
                    {
                        // Агентское вознаграждение
                        ds.PayPayment(ds.CreateInnerTransfer(
                            accountName: "Остаток.Вознаграждения",
                            direction: TransferDirection.Input,
                            companyId: companyId,
                            partnerId: partner.InviterId.Value,
                            documentId: partnerId,
                            article: "Реферальное вознаграждение",
                            date: date,
                            sum: sumForPay,//sum * 0.1m,
                            paymentMethod: PaymentMethod.Inner,
                            comment: $"Реферальное вознаграждение за {partner.ObjectName}",
                            documentType: null,
                            user: user), date);
                    }
                    LogPayments(refLog, true);
                    //}
                }


                ds.UpdatePartnerActivity(partnerId, true);

                return documentId;
            }
        }


        /* old
         public uint BuyInvestment(
            uint companyId,
            uint camulativeMarketingId,
            uint partnerId,
            decimal sum,
            bool isProlonged,
            bool isCreateNewPlace,
            DateTime date,
            string user)
        {
            lock (lockObj)
            {
                IDataService ds = new KbtDataService();

                var partner = ds.GetPartner(partnerId);
                if ((partner.BalanceInner ?? 0) < sum)
                    throw new Exception("Недостаточно средств на лицевом счете!");

                //var inviter = ds.GetPartner(partner.InviterId.Value);

                // Поиск инвест программы
                InvestProgram program = ds.GetInvestProgram(sum);
                if (program == null)
                    throw new Exception("Не найдена программа удовлетворяющая условиям");

                if (!this.IsRightInvestSum(sum, program.MinSum, program.MaxSum, program.Step))
                    throw new Exception("Некоректно указана сумма!");

                #region Создание мест




                // Если мест нет, либо создается последующие
                if (ds.GetPlaceCount(camulativeMarketingId, partnerId) == 0 || isCreateNewPlace)
                {
                    this.CreateCamulativeProgramPlace(
                        ds: ds,
                        companyId: companyId,
                        marketingId: camulativeMarketingId,
                        partnerId: partnerId,
                        objectName: partner.ObjectName,
                        alias: partner.Login,
                        sum: sum,
                        date: date,
                        user: user);
                }
                else
                {
                    // Повторная оплата по месту
                    this.RePayPlace(
                        ds: ds,
                        partnerId: partnerId,
                        companyId: companyId,
                        marketingId: camulativeMarketingId,
                        date: date,
                        user: user);
                }

                #endregion

                // создание договора
                var documentId = this.CreateDocumnt(
                    ds: ds,
                    companyId: companyId,
                    partnerId: partnerId,
                    programId: program.id_object,
                    percent: program.YearPercent,
                    date: date,
                    sum: sum,
                    isProlonged: isProlonged,
                    user: user);

                // списание с лицевого счета
                ds.PayPayment(ds.CreateInnerTransfer(
                    accountName: "Остаток.ВнутреннийСчет",
                    direction: TransferDirection.Output,
                    companyId: companyId,
                    partnerId: partnerId,
                    documentId: documentId,
                    article: $"Открытие инвест программы",
                    date: date,
                    sum: sum,
                    paymentMethod: PaymentMethod.Inner,
                    comment: $"Открытие инвест программы {program.ObjectName}",
                    documentType: null,
                    user: user), date);

                // Агентское вознаграждение
                ds.PayPayment(ds.CreateInnerTransfer(
                    accountName: "Остаток.Вознаграждения",
                    direction: TransferDirection.Input,
                    companyId: companyId,
                    partnerId: partner.InviterId.Value,
                    documentId: partnerId,
                    article: "Реферальное вознаграждение",
                    date: date,
                    sum: sum * 0.1m,
                    paymentMethod: PaymentMethod.Inner,
                    comment: $"Реферальное вознаграждение за {partner.ObjectName}",
                    documentType: null,
                    user: user), date);

                ds.UpdatePartnerActivity(partnerId, true);

                return documentId;
            }
        }*/

        public uint CreateTechPlace(
            uint companyId,
            uint marketingId,
            DateTime date,
            string user)
        {
            lock (lockObj)
            {
                var ds = new KbtDataService();
                return this.CreateCamulativeProgramPlace(
                    ds: ds,
                    companyId: companyId,
                    marketingId: marketingId,
                    partnerId: 0,
                    objectName: "Техническое место",
                    alias: "Техническое место",
                    sum: 300,
                    date: date,
                    user: user,
                    pos: new PlacePos());
            }
        }

        public uint CreatePlace(
            uint companyId,
            uint marketingId,
            uint partnerId,
            string objectName,
            string alias,
            decimal sum,
            DateTime date,
            string user,
            uint activePlaceId,
            int pos)
        {
            lock (lockObj)
            {
                var ds = new KbtDataService();
                PlacePos position = new PlacePos();
                position.Pos = pos;
                position.Parent = ds.GetPlace(activePlaceId);

                return this.CreateCamulativeProgramPlace(
                    ds: ds,
                    companyId: companyId,
                    marketingId: marketingId,
                    partnerId: partnerId,
                    objectName: objectName,
                    alias: alias,
                    sum: sum,
                    date: date,
                    user: user,
                    pos: position);
            }
        }


        public uint CreateCamulativeProgramPlace(
            IDataService ds,
            uint companyId,
            uint marketingId,
            uint partnerId,
            string objectName,
            string alias,
            decimal sum,
            DateTime date,
            string user,
            PlacePos pos)
        {
            #region Создание места в накопительной программе

            /**********************************************
            * Создания места
            * + Определить активное место у контрагента
            * + Поиск вышестоящего места
            * + Создание места
            *************************************************/


            var structure = ds.GetStructure(marketingId);

            var companyPlace = structure.OrderBy(x => x.hash.Length).First();
            var firstPlace = structure.SingleOrDefault(x => x.id_parent == companyPlace.id_object);

            MarketingPlace root = firstPlace ?? companyPlace;
            var partner = new Partner();
            if (partnerId > 0)
            {
                partner = ds.GetPartner(partnerId);

                // Если есть места у самого партнера, то под него
                // если нет, то под спонсора
                var inviterPlaces = structure.Where(x => x.PartnerId.Value == partnerId);
                if (inviterPlaces.Count() == 0)
                {
                    inviterPlaces = structure.Where(x => x.PartnerId.Value == partner.InviterId.Value);
                }

                if (inviterPlaces.Count() == 0)
                    throw new Exception("У спонсора нет мест в накопительной программе!");


                root = this.GetActivePlace(inviterPlaces, structure);

                // Если корень - компания и место под компанией уже занято,
                // то корень - занятое место под компанией
                if (root.id_object == companyPlace.id_object &&
                    firstPlace != null)
                {
                    root = firstPlace;
                }

            }

            // Поиск позиции под корневым местом
            if (pos.Parent == null)
            {
                /*var*/
                pos = FindPos(root, structure);
            }

            uint newPlaceId = ds.CreatePlace(
                marketingId: marketingId,
                partnerId: partnerId,
                parentId: pos.Parent.id_object,
                pos: pos.Pos,
                entrySum: sum,
                isActive: true,
                alias: alias,
                name: objectName,
                rank: null,
                user: user);

            var newPlace = ds.GetPlace(newPlaceId);
            ds.SetPlaceChildCount(pos.Parent.id_object, (pos.Parent.ChildCount ?? 0) + 1);

            ds.SetPartnerMarketPlaceStatus(partnerId, true);

            // если распределен и песочницы и у него куплен пакет инвестиций, то нужно обновить объем вышестоящим
            if (pos.Parent != null && partnerId > 0 && partner.BalanceInvestments > 0)
            {
                structure = ds.GetStructure(marketingId);
                if (structure.Count() > 0 && newPlace != null)
                {
                    UpdateParentInvestShoulderSum(newPlace, structure, sum);
                }
            }

            #endregion

            #region Пригласителю
            /***************************************************** 
            * Пригласителю:
            * + Начисление реф бонуса
            * + Проверяется есть ли средства на накоп счете, 
            *   если есть то сколько можно переводить
            *****************************************************/

            // Проверка накоплений у пригласителя
            /* var referCount = ds.GetReferCount(marketingId, root.PartnerId.Value);
             var savings = ds.GetStructSavings(root.id_object);
             foreach (var saving in savings)
             {
                 if (this.ChargeOnTheMainWallet(referCount, saving.Level.Value))
                 {
                     ds.PayPayment(saving.id_object, date);
                 }
             }*/


            #endregion

            #region в 5 вышестоящих уровней

            /****************************************************************************** 
                * В 5 вышестоящих уровней
                *  + Проверка последние 5 из 62. Если нет, то 
                *      + определение на какой кошелек
                *  
                *  + у последнего (5го) уровня проверка заполненности всех 5ти уровней.
                *    Если заполнены, то проверяем есть ли остаток на накопительном счете. Если есть
                *    то обнуляем и создаем инвест пакет
                ****************************************************************************/

            // начисляем в 5 уровней вверх
            // Начисление в пять уровней
            /*structure = ds.GetStructure(marketingId);
            var parent = newPlace;
            for (var i = 0; i < GetParentCount(newPlace, structure, 5); i++)
            {
                parent = this.GetParent(parent, structure, 1);
                var structValue = this.CalcStructValue(parent, structure, 5);

                if (structValue < 58)
                {
                    if ((parent.PartnerId ?? 0) > 0)
                    {
                        var id_p = ds.CreateInnerTransfer(
                            accountName: "Остаток.Вознаграждения",
                            direction: TransferDirection.Input,
                            companyId: companyId,
                            partnerId: parent.PartnerId.Value,
                            documentId: newPlaceId,
                            article: "Вознаграждение по накопительной программе",
                            date: date,
                            sum: 50,
                            paymentMethod: PaymentMethod.Inner,
                            comment: $"Вознаграждение по накопительной программе за {objectName}",
                            documentType: "СтруктурныйПлатеж",
                            user: user);

                        ds.SetStructPaymentDetails(id_p, parent.id_object, i + 1);
                        if (this.ChargeOnTheMainWallet(
                            ds.GetReferCount(marketingId, parent.PartnerId.Value),
                            i + 1))
                        {
                            ds.PayPayment(id_p, date);
                        }
                    }
                }
                else
                {
                    if ((parent.PartnerId ?? 0) > 0)
                    {
                        RePayParent(
                            ds: ds,
                            companyId: companyId,
                            place: parent,
                            orderId: newPlaceId,
                            level: structValue - 57,
                            structure: structure,
                            date: date,
                            user: user);
                    }
                    else 
                    {
                        for (var j = 0; j < 11; j++)
                        {
                            this.CreateTechPlace(
                                companyId: companyId,
                                marketingId: marketingId,
                                date: date,
                                user: user);
                        }

                                
                    }
                }
            }










        // для 5-го уровня проверяем
        var parent5Id = this.GetParent(newPlace, structure, 5);
        if (parent5Id != null)
        {
            if (this.IsFilled(parent5Id, structure))
            {
                savings = ds.GetStructSavings(parent5Id.id_parent);
                if (savings.Count() > 0)
                {
                    InvestProgram program = ds.GetInvestProgram(sum);

                    this.CreateDocumnt(
                        ds: ds,
                        companyId: companyId,
                        partnerId: partnerId,
                        programId: program.id_object,
                        percent: program.YearPercent,
                        date: date,
                        sum: 1000,
                        isProlonged: false,
                        user: user);

                    foreach (var payment in savings)
                        ds.RemovePayment(payment.id_object);
                }
            }
        }*/

            #endregion

            #region __Выводы

            /*  
            * 
            * Динамически считать начисления 50 по уровням по местам, не получится,
            * так как за это место кто-то уже получил
            * 
            * 
            * На накопительном счете всегда деньги активного места, так как:
            * - место становится "неактивным" только при заполнении 5 уровней
            * - если при заполненнии 5 уровней нет 3х приглашенных, то средства на счете обнуляются в пользу инвест пакета
            * 
            */

            #endregion

            return newPlaceId;
        }


        //public void SetNullBalance(decimal sum)
        //{
        //    IDataService ds = new KbtDataService();
        //    var partners = ds.GetPartners();
        //    using (var client = new WebDataClient())
        //    {
        //        int count = partners.Count();
        //        int i = 0;
        //        foreach (var item in partners)
        //        {
        //            var values = new Dictionary<string, object>();
        //            values.Add("Остаток.ВнутреннийСчет", null);
        //            values.Add("Остаток.Вознаграждения", null);
        //            values.Add("Остаток.ИнвестиционныйСчет", null);
        //            values.Add("Остаток.Проценты", null);
        //            client.SetObjectValues(item.id_object, values);
        //            i++;
        //        }
        //    }

        //    if (sum > 0)
        //    {
        //        foreach (var partner in partners)
        //        {
        //            ds.PayPayment(ds.CreateInnerTransfer(
        //           accountName: "Остаток.ВнутреннийСчет",
        //           direction: TransferDirection.Input,
        //           companyId: 0,
        //           partnerId: partner.id_object,
        //           documentId: partner.id_object,
        //           article: "Пополнение счета",
        //           date: DateTime.Now,
        //           sum: sum,
        //           paymentMethod: PaymentMethod.Inner,
        //           comment: $"Пополнение счета",
        //           documentType: null,
        //           user: ""), DateTime.Now);
        //        }
        //    }
        //}


        /*  void RePayParent(IDataService ds, uint companyId, MarketingPlace place, uint orderId,
              IEnumerable<MarketingPlace> structure, int level, DateTime date, 
              string user)
          {
              var referCount = ds.GetReferCount(place.MarketingId.Value, place.PartnerId.Value);

              if (!this.IsFilled(place, structure) || referCount >= 3)
              {
                  var id_p = ds.CreateInnerTransfer(
                     accountName: "Остаток.Вознаграждения",
                     direction: TransferDirection.Input,
                     companyId: companyId,
                     partnerId: place.PartnerId.Value,
                     documentId: orderId,
                     article: "Вознаграждение по накопительной программе",
                     date: date,
                     sum: 50,
                     paymentMethod: PaymentMethod.Inner,
                     comment: $"Вознаграждение по накопительной программе за {place.ObjectName}",
                     documentType: null,
                     user: user);

                  ds.SetStructPaymentDetails(id_p, place.id_object, level);

                  if (this.ChargeOnTheMainWallet(referCount, level))
                  {
                      ds.PayPayment(id_p, date);
                  }
              }
          }

          uint RePayPlace(IDataService ds, uint partnerId,
              uint companyId, uint marketingId, DateTime date, string user)
          {
              var places = ds.GetPlaces(marketingId, partnerId);
              if (places.Count() == 0)
                  throw new Exception("У спонсора нет мест в накопительной программе!");

              var structure = ds.GetStructure(marketingId);
              var activePlace = this.GetActivePlace(places, structure);

              for (var i = 0; 
                  i < this.GetParentCount(activePlace, structure, 5); i++)
              {
                  var parent = this.GetParent(activePlace, structure, i + 1);

                  RePayParent(
                      ds: ds,
                      companyId: companyId,
                      place: parent,
                      orderId: activePlace.id_object,
                      level: i + 1,
                      structure: structure,
                      date: date,
                      user: user);
              }

              return activePlace.id_object;
          }*/

        uint CreateDocumnt(IDataService ds, uint companyId, uint partnerId,
            uint programId, decimal percent,
            DateTime date, decimal sum, bool isProlonged, string user)
        {

            /* System.Collections.ArrayList list = new System.Collections.ArrayList();
             list.AddRange(new uint[] { 76, 86, 83, 292, 27396, 89, 14195, 274, 84, 24802, 293, 88, 90, 91, 275, 276, 277, 279, 280 });
             if (list.Contains(partnerId))
             {
                 date = DateTime.ParseExact("2019-05-06", "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
             }*/


            var documentId = ds.CreateInvestment(
                companyId: companyId,
                programId: programId,
                partnerId: partnerId,
                sum: sum,
                isProlonged: isProlonged,
                date: date,
                user: user);

            /*var investments = ds.GetInvestments(partnerId, "Закрыт");
            if (investments.Count() > 0)
            {
                foreach(var item in investments)
                {
                    TerminateInvestment(companyId, partnerId, item.id_object,0,DateTime.Now,user,false);
                }
            }*/

            /*
             * Т.К. в любом случае будут объекты-начисления на счет, то
             * нет разницы создавать их сейчас или по факту
             * начинаем платить с определенного в конфиге дня
             */

            int currentDayNumber = (int)date.DayOfWeek;
            var company = ds.GetPartner(5);
            int countAddDay = 0;
            if (company != null)
            {
                countAddDay = company.Settings_StartPayWorkDay ?? 0;
            }

            /*int startWorkPayDay = ((countAddDay + currentDayNumber) % 7);
            if (startWorkPayDay == 6)
            {
                countAddDay += 2;
            }
            else if (startWorkPayDay == 7)
            {
                countAddDay += 1;
            }*/

            countAddDay = countAddDay > 0 ? countAddDaysForStartDate(date, countAddDay) : 0;
            DateTime startDate = date.AddDays(countAddDay);
            int workDays = countWorkDays(startDate);

            var days = Convert.ToInt32((startDate.Date.AddYears(1) - startDate.Date).TotalDays);
            var reminder = percent * sum + sum;
            var investment = ds.GetInvestment(documentId);
            int j = 0;
            for (var i = 0; i < days; i++)
            {
                //var s = Math.Round(reminder / (days - i), 2);
                //if (i == days)
                //    s = reminder;

                if (i > 0)
                {
                    startDate = startDate.Date.AddDays(1);
                }

                if (startDate.Date.DayOfWeek != DayOfWeek.Sunday && startDate.Date.DayOfWeek != DayOfWeek.Saturday)
                {

                    decimal s = 0;
                    if (j == workDays)
                    {
                        s = reminder;
                    }
                    else
                    {
                        s = Math.Round(reminder / (workDays - j), 2);
                    }
                    ds.CreateInnerTransfer(
                           accountName: "Остаток.Проценты",
                           direction: TransferDirection.Input,
                           companyId: companyId,
                           partnerId: partnerId,
                           documentId: documentId,
                           article: "Начисление процентов и тела",
                           date: startDate,/* date.Date.AddDays(i + 1),*/
                           sum: s,
                           paymentMethod: PaymentMethod.Inner,
                           comment: $"Начисление процентов и тела за {investment.ProgramName}",
                           documentType: "ОжидаемыйПлатеж",
                           user: user);

                    reminder -= s;
                    j++;
                }
            }

            // Возврат инвестиций
            /* ds.CreateInnerTransfer(
                    accountName: "Остаток.ВнутреннийСчет",
                    direction: TransferDirection.Input,
                    companyId: companyId,
                    partnerId: partnerId,
                    documentId: documentId,
                    article: $"Закрытие инвест программы",
                    date: date.AddDays(days),
                    sum: sum,
                    paymentMethod: PaymentMethod.Inner,
                    comment: $"Закрытие инвест программы {investment.ProgramName}",
                    documentType: "ОжидаемыйПлатеж",
                    user: user);*/


            // Начисление на инвест счет
            ds.PayPayment(ds.CreateInnerTransfer(
                accountName: "Остаток.ИнвестиционныйСчет",
                direction: TransferDirection.Input,
                companyId: companyId,
                partnerId: partnerId,
                documentId: documentId,
                article: $"Открытие инвест программы",
                date: date,
                sum: sum,
                paymentMethod: PaymentMethod.Inner,
                comment: $"Открытие инвест программы {investment.ProgramName}",
                documentType: null,
                user: user), date);

            return documentId;
        }

        public int countAddDaysForStartDate(DateTime date, int startPayDay)
        {
            // т.к. на след день то +1
            int countAddDays = 1;
            int dayOfWeek = (int)date.Date.DayOfWeek + 1;
            int currentCountDay = 0;
            while (currentCountDay < startPayDay)
            {
                if (dayOfWeek++ < 6)
                {
                    currentCountDay++;
                }
                countAddDays++;
                if (dayOfWeek > 7)
                {
                    dayOfWeek = 1;
                }
            }

            if (dayOfWeek == 6)
            {
                countAddDays += 2;
            }
            else if (dayOfWeek == 7)
            {
                countAddDays += 1;
            }
            return countAddDays;
        }


        public int countWorkDays(DateTime date)
        {
            DateTime endDate = date.AddYears(1);
            int dayOfWeek = (int)date.Date.DayOfWeek;
            if (dayOfWeek == 0)
            {
                dayOfWeek = 7;
            }
            int endDayOfweek = (int)endDate.DayOfWeek;
            if (endDayOfweek == 0)
            {
                endDayOfweek = 7;
            }
            int countWeekStartDays = dayOfWeek > 0 ? 8 - dayOfWeek : 7;
            int countWeekEndDays = endDayOfweek;

            int totalDays = (int)(endDate - date).TotalDays;
            totalDays -= (countWeekStartDays + countWeekEndDays);
            int workDays = (int)((totalDays + 1) * 5 / 7);
            workDays += dayOfWeek < 6 ? 6 - dayOfWeek : 0;
            workDays += endDayOfweek < 6 ? endDayOfweek : 5;
            if (endDayOfweek < 6)
            {
                workDays--;
            }

            return workDays;
        }


        #region Вспомогательные методы

        // Получем активное место партнера
        // tested 27.10.2018 00:25
        public MarketingPlace GetActivePlace(IEnumerable<MarketingPlace> partnerPlaces,
            IEnumerable<MarketingPlace> structure)
        {
            var sorted = partnerPlaces.OrderBy(x => x.hash.Length).ThenBy(x => x.hash);
            foreach (var place in sorted)
            {
                if (!this.IsFilled(place, structure))
                    return place;
            }
            return sorted.ElementAt(0);
        }


        public Dictionary<uint, string> GetBinaryHashTable(IEnumerable<MarketingPlace> structure)
        {
            var ordered = structure.OrderBy(x => x.hash.Length);
            Dictionary<uint, string> invent = new Dictionary<uint, string>();
            invent.Add(ordered.First().id_object, "1");
            int count = ordered.Count();
            for (var i = 1; i < count; i++)
            {
                var item = ordered.ElementAt(i);
                var hash = invent[item.id_parent] + item.SortPosition.ToString();
                invent.Add(item.id_object, hash);
            }
            return invent;
        }
        public Dictionary<string, uint> Invent(Dictionary<uint, string> items)
        {
            Dictionary<string, uint> dictionary = new Dictionary<string, uint>();
            foreach (var item in items)
            {
                dictionary.Add(item.Value, item.Key);
            }
            return dictionary;
        }



        // Возврает позицию для нового места
        public PlacePos FindPos(MarketingPlace place, IEnumerable<MarketingPlace> structure)
        {
            var id_hash = this.GetBinaryHashTable(structure);
            var hash_id = this.Invent(id_hash);

            string inviterHashCode = id_hash[place.id_object];

            var bh = new BinaryMarketingHelper();
            var newHash = bh.GetFreeHashCode(inviterHashCode, hash_id.Keys);
            uint parentId = hash_id[newHash.Substring(0, newHash.Length - 1)];
            return new PlacePos
            {
                Pos = bh.GetPosByHashCode(newHash),
                Parent = structure.Single(x => x.id_object == parentId)
            };
        }

        // Возвращает количество мест в подструктуре
        public int CalcStructValue(MarketingPlace place,
            IEnumerable<MarketingPlace> structure, int levels)
        {
            var l = place.hash.Length + levels * 5;
            return structure.Count(x =>
                x.hash.StartsWith(place.hash) &&
                x.hash != place.hash &&
                x.hash.Length <= l);
        }

        // Возвращает вышестоящего в уровне
        // tested 26.10.2018 23:05
        public MarketingPlace GetParent(MarketingPlace place, IEnumerable<MarketingPlace> structure,
            int level)
        {
            var parent = place;
            int l = -1;
            while (parent != null && ++l < level)
                parent = structure.SingleOrDefault(x => x.id_object == parent.id_parent);
            return parent;
        }

        // Проверка заполненности 5ти уровней места
        // tested 26.10.2018 23:52
        public bool IsFilled(MarketingPlace place, IEnumerable<MarketingPlace> structure)
        {
            for (var i = 1; i <= 5; i++)
            {
                var l = place.hash.Length + i * 5;
                if (structure.Count(x =>
                    x.hash.StartsWith(place.hash) &&
                    x.hash.Length == l) != Math.Pow(2, i))
                    return false;
            }
            return true;
        }

        // tested 26.10.2018 18:39
        public int GetParentCount(BaseObject place,
            IEnumerable<BaseObject> structure,
            int max)
        {
            var parent = place;
            int l = -1;
            while (parent != null && ++l < max)
                parent = structure.SingleOrDefault(x => x.id_object == parent.id_parent);

            return l;
        }

        // Возвращает можно ли начислять на основной кошелек $50
        // tested 17.10.2018 18:52
        public bool ChargeOnTheMainWallet(int referCount, int level)
        {
            if (referCount >= 3)
                return true;

            if (level > 0 && level < 3)
            {
                return referCount >= 1 ? true : false;
            }
            else if (level == 3)
            {
                return referCount >= 2 ? true : false;
            }
            else
            {
                return referCount >= 3 ? true : false;
            }
        }

        // Проверяет верно ли указана инвест сумма
        public bool IsRightInvestSum(decimal sum, decimal min, decimal max, decimal step)
        {
            if (sum < min || sum > max)
                return false;

            if (sum % step != 0)
                return false;

            return true;
        }

        // Можно ли партнеру создать еще место
        // tested 25.10.2018 11:37
        public bool IsAllowCreateNewPlace(int placeCount, int referCount)
        {
            if (placeCount == 0)
                return false;

            return referCount / placeCount >= 4;
        }

        #endregion
        #region Инвест бонусы

        // Вовзращает число уровней, с которых получает партнер от дохода инвестиций
        // tested 23.10.2018 12:57
        public int GetInvestBonusLevels(int rank)
        {
            if (rank == 1)
                return 6;
            else if (rank == 2)
                return 5;
            else if (rank == 3)
                return 4;
            else if (rank == 4)
                return 3;
            else
                return 2;
        }

        // tested 24.10.2018 14:00
        // Возвращает список родителей, кто должен получить вознаграждение
        public IEnumerable<decimal> TransformInfinityBonusPercent(List<decimal> percents)
        {
            decimal max = 0;
            foreach (var p in percents)
            {
                var diff = p - max;
                yield return diff > 0 ? diff : 0;

                if (p > max)
                    max = p;
            }
        }

        // tested 24.10.2018 14:09
        // Получить проценты бонуса бесконечности
        public decimal GetInfinityPercent(int count)
        {
            if (count >= 6)
                return 0.1m;
            else if (count >= 5)
                return 0.08m;
            else if (count >= 4)
                return 0.06m;
            else if (count >= 3)
                return 0.05m;
            else
                return 0;
        }

        // tested 24.10.2018 14:09
        // Возвращает ранг от объема первой линии + свой
        public int GetInvestRank(decimal value)
        {
            if (value >= 100e3m)
                return 1;
            else if (value >= 65e3m)
                return 2;
            else if (value >= 35e3m)
                return 3;
            else if (value >= 15e3m)
                return 4;
            else if (value >= 1e3m)
                return 0;
            else
                return -1;
        }

        // Цепочка родителей с их %
        public List<decimal> GetInfinityPercentChain(Dictionary<uint, uint> parents, Dictionary<uint, decimal> percents, uint partnerId)
        {
            List<decimal> result = new List<decimal>();
            var parentId = parents[partnerId];
            while (parentId > 0)
            {
                result.Add(percents[parentId]);
                parentId = parents[parentId];
            }
            return result;
        }


        // рассчитывает проценты всей структуры по рангам
        public Dictionary<uint, decimal> CalcInfinityPercents(Dictionary<uint, uint> parents, Dictionary<uint, int> ranks)
        {
            Dictionary<uint, decimal> percents = new Dictionary<uint, decimal>();
            foreach (var p in parents)
            {
                if (ranks[p.Key] != 1)
                {
                    percents.Add(p.Key, 0);
                    continue;
                }

                int count = 0;
                foreach (var pp in parents)
                {
                    if (pp.Value == p.Key && ranks[pp.Key] == 1)
                        count++;
                }

                percents.Add(p.Key, this.GetInfinityPercent(count));
            }
            return percents;
        }

        // tested: 24.10.2018 16:01
        // Рассчитывает объем (свой + певая линия)
        public Dictionary<uint, decimal> CalcInvestValues(Dictionary<uint, uint> parents, Dictionary<uint, decimal> investments)
        {
            Dictionary<uint, decimal> values = new Dictionary<uint, decimal>();
            foreach (var p in parents)
            {
                if (!values.ContainsKey(p.Key))
                    values.Add(p.Key, investments[p.Key]);
                else
                    values[p.Key] += investments[p.Key];

                if (p.Value > 0)
                {
                    if (!values.ContainsKey(p.Value))
                        values.Add(p.Value, investments[p.Key]);
                    else
                        values[p.Value] += investments[p.Key];
                }
            }
            return values;
        }

        // Рассчитывает ранги
        public Dictionary<uint, int> CalcInvestRanks(Dictionary<uint, decimal> values)
        {
            Dictionary<uint, int> ranks = new Dictionary<uint, int>();
            foreach (var v in values)
            {
                ranks.Add(v.Key, this.GetInvestRank(v.Value));
            }

            return ranks;
        }




        public void PayInvestPercents(DateTime date, uint companyId, string user)
        {
            lock (lockObj)
            {
                IDataService ds = new KbtDataService();

                /*
                 * + Вычислить объем
                 * + Вычислить ранги всей структуры: нужны объемы первой линии
                 * + Получить проценты по всей структуре: нужны parents и ранги
                 * 
                 * По каждому
                 * + Получаем цепочку % по заданному партнеру: нужны Parents и % по каждому
                 * + Трансформируем цепочку и по ней начисляем
                 */

                //Остаток.Проценты
                //TransferDirection.Input
                //Остаток.Вознаграждения
                //Вознаграждение со структуры
                //Начисление процентов и тела

                var partnerInvestments = new Dictionary<uint, PartnerInvestData>();
                PartnerInvestData partnerInvestData = new PartnerInvestData();
                var expected = ds.GetExpectedPayments(date);
                if (expected.Count() > 0)
                {
                    foreach (var payment in expected)
                    {
                        if (!partnerInvestments.ContainsKey(payment.PartnerId ?? 0))
                        {
                            partnerInvestData = createPartnerInvestData(payment.PartnerId ?? 0, ds);
                            partnerInvestments.Add(payment.PartnerId ?? 0, partnerInvestData);
                        }
                        else
                        {
                            partnerInvestData = partnerInvestments[payment.PartnerId ?? 0];
                        }
                        //"---оборот левого плеча по бинару\r\n---оборот правого плеча по бинару"
                        //string partnerLog = $"--{DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss")}\r\n—-Фио/логин { ShortName(partnerInvestData.partner)}/{partnerInvestData.partner.Login} \r\n";
                        decimal partSumForPay = payment.PaymentSum ?? 0;
                        /*decimal totalPaidSum = partSumForPay + partnerInvestData.AllPaymentsSum;
                        decimal extraSum = 0;
                        decimal doubleInvestSum = partnerInvestData.BalanceInvestments * 2;

                        partnerLog += $"---Начисление %\r\n---проверка двухкратной выплаты суммы. План. платеж по %:{partSumForPay.ToString()}, Всего:{partnerInvestData.AllPaymentsSum.ToString()} Двукратная сумма:{doubleInvestSum.ToString()};";
                        // проверка двухкратной выплаты суммы
                        if (totalPaidSum > doubleInvestSum)
                        {
                            partnerInvestData.isCanPay = false;
                            extraSum = totalPaidSum - doubleInvestSum;
                            
                            if (partnerInvestData.AllPaymentsSum < doubleInvestSum)
                            {
                                partSumForPay -= extraSum;//doubleInvestSum - partnerInvestData.AllPaymentsSum;
                                //partnerLog += $"Платеж: {partSumForPay.ToString()}";
                            }
                            partnerLog += $"Компании:{extraSum.ToString()};Платеж: {partSumForPay.ToString()}\r\n;Закрытие инвест пакета\r\n";                            
                            
                            closeInvestment(companyId, payment.PartnerId ?? 0, payment.OrderId ?? 0, DateTime.Now, user, ds);
                        }

                        partnerLog += $"---Начисление %\r\n---проверка лимита доходности. Лимит:{partnerInvestData.MaxLimit.ToString()} Все начисления + текущий платеж сумма:{(partnerInvestData.AllPaymentsSum + partSumForPay).ToString()};";
                        //проверка лимита доходности, если есть
                        if (partnerInvestData.isCanPay && partnerInvestData.MaxLimit > 0)
                        {
                            if (partnerInvestData.AllPaymentsSum + partSumForPay > partnerInvestData.MaxLimit)
                            {
                                partnerInvestData.isCanPay = false;
                                extraSum += partnerInvestData.AllPaymentsSum + partSumForPay - partnerInvestData.MaxLimit;                                
                                if (partnerInvestData.AllPaymentsSum < partnerInvestData.MaxLimit)
                                {
                                    partSumForPay -= partnerInvestData.AllPaymentsSum + partSumForPay - partnerInvestData.MaxLimit;//partnerInvestData.MaxLimit - partnerInvestData.AllPaymentsSum;
                                    //partnerLog += $"Платеж: {partSumForPay.ToString()}";
                                }
                                partnerLog += $"Компании:{extraSum.ToString()};Платеж: {partSumForPay.ToString()}\r\n";                                
                                closeInvestment(companyId, payment.PartnerId ?? 0, payment.OrderId ?? 0, DateTime.Now, user, ds);
                            }
                        }*/

                        //if (partnerInvestData.isCanPay)
                        {
                            ds.PayPayment(payment.id_object, date);
                        }
                       /* else if (extraSum > 0)
                        {                            
                            ds.PayPayment(ds.CreateInnerTransfer(
                            accountName: "Остаток.Проценты",
                            direction: TransferDirection.Input,
                            companyId: companyId,
                            partnerId: companyId,
                            documentId: payment.OrderId ?? 0,
                            article: "Начисление процентов и тела",
                            date: date,
                            sum: extraSum,
                            paymentMethod: PaymentMethod.Inner,
                            comment: $"Начисление процентов и тела за {payment.PartnerObjectName}",
                            documentType: null,
                            user: user), date);
                        }
                        if (partSumForPay > 0)
                        {
                            ds.PayPayment(ds.CreateInnerTransfer(
                            accountName: "Остаток.Проценты",
                            direction: TransferDirection.Input,
                            companyId: companyId,
                            partnerId: payment.PartnerId ?? 0,
                            documentId: payment.OrderId ?? 0,
                            article: "Начисление процентов и тела",
                            date: date,
                            sum: partSumForPay,
                            paymentMethod: PaymentMethod.Inner,
                            comment: $"Начисление процентов и тела за {payment.PartnerObjectName}",
                            documentType: null,
                            user: user), date);

                            partnerInvestData.AllPaymentsSum += partSumForPay;
                            partnerInvestments[payment.PartnerId ?? 0] = partnerInvestData;
                        }
                        LogPayments(partnerLog);*/
                        /*if (!partnerInvestData.isCanPay)
                        {
                            LogPayments(partnerLog);
                        }*/
                    }
                }

                var structure = ds.GetStructure(93);
                if (structure.Count() > 0)
                {
                    foreach (var item in structure)
                    {
                        if (!partnerInvestments.ContainsKey(item.PartnerId ?? 0))
                        {
                            partnerInvestData = createPartnerInvestData(item.PartnerId ?? 0, ds);
                            partnerInvestments.Add(item.PartnerId ?? 0, partnerInvestData);
                        }
                        else
                        {
                            partnerInvestData = partnerInvestments[item.PartnerId ?? 0];
                        }
                        string partnerLog = $"--{DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss")}\r\n—-Фио/логин {ShortName(item)}/{(partnerInvestData.partner != null ? partnerInvestData.partner.Login : "")} \r\n";

                        decimal currentPaymentSum = 0;
                        item.PartnerBinaryLeftShoulderSum = item.PartnerBinaryLeftShoulderSum != null ? (decimal)item.PartnerBinaryLeftShoulderSum : 0;
                        item.PartnerBinaryRightShoulderSum = item.PartnerBinaryRightShoulderSum != null ? (decimal)item.PartnerBinaryRightShoulderSum : 0;
                        if (item.PartnerBinaryLeftShoulderSum > item.PartnerBinaryRightShoulderSum)
                        {
                            currentPaymentSum = item.PartnerBinaryRightShoulderSum ?? 0;
                        }
                        else
                        {
                            currentPaymentSum = item.PartnerBinaryLeftShoulderSum ?? 0;
                        }

                        decimal partSumForPay = currentPaymentSum * 0.1m;
                        //decimal totalPaidSum = partSumForPay + partnerInvestData.AllPaymentsSum;
                        decimal extraSum = 0;
                        //decimal doubleInvestSum = partnerInvestData.BalanceInvestments * 2;
                        partnerLog += $"---оборот левого плеча по бинару:{ item.PartnerBinaryLeftShoulderSum.ToString()}\r\n--- оборот правого плеча по бинару:{item.PartnerBinaryRightShoulderSum.ToString()}\r\n---сумма к начислению по выбранному обороту:{partSumForPay.ToString()}\r\n";
                        if (partnerInvestData.isCanPay && partSumForPay > 0)
                        {
                            //проверка дневного лимита
                            if (partnerInvestData.isCanPay)
                            {
                                partnerLog += $"---проверка по лимиту на сутки. Лимит:{partnerInvestData.Program.DayLimit.ToString()};Сумма оплаченного сегодня:{partnerInvestData.DayBinaryPaymentsSum.ToString()};";
                                if (partSumForPay + partnerInvestData.DayBinaryPaymentsSum >= partnerInvestData.Program.DayLimit)
                                {
                                    partnerInvestData.isCanPay = false;
                                    decimal diffDaySum = partSumForPay + partnerInvestData.DayBinaryPaymentsSum - partnerInvestData.Program.DayLimit;
                                    extraSum = diffDaySum > partSumForPay ? partSumForPay : diffDaySum;
                                    partSumForPay -= extraSum;
                                    //partnerLog += $"Компании:{extraSum.ToString()},Платеж: {partSumForPay.ToString()}";
                                }
                                partnerLog += $"Компании:{extraSum.ToString()},Платеж: {partSumForPay.ToString()}\r\n";
                                //partnerLog += "\r\n";
                                partnerLog += $"---проверка по лимиту на месяц. Лимит:{partnerInvestData.Program.MonthLimit.ToString()};Сумма оплат за месяц:{partnerInvestData.MonthBinaryPaymentsSum.ToString()};";
                                if (partSumForPay + partnerInvestData.MonthBinaryPaymentsSum >= partnerInvestData.Program.MonthLimit)
                                {
                                    decimal monthDiffSum = partSumForPay + partnerInvestData.MonthBinaryPaymentsSum - partnerInvestData.Program.MonthLimit;
                                    extraSum += monthDiffSum > partSumForPay ? partSumForPay : monthDiffSum;
                                    partSumForPay -= monthDiffSum > partSumForPay ? partSumForPay : monthDiffSum;
                                    partnerInvestData.isCanPay = false;
                                    //partnerLog += $"Компании:{extraSum.ToString()}Платеж: {partSumForPay.ToString()}";
                                }
                                partnerLog += $"Компании:{extraSum.ToString()}Платеж: {partSumForPay.ToString()}\r\n";
                                //partnerLog += "\r\n";
                            }

                            partnerLog += $"---проверка лимита доходности. Лимит:{partnerInvestData.MaxLimit.ToString()} Все платежи + текущий платеж сумма:{(partnerInvestData.AllPaymentsSum + partSumForPay).ToString()};";
                            bool isClosePackage = false;
                            //проверка лимита доходности, если есть
                            if (/*partnerInvestData.isCanPay &&*/ partnerInvestData.MaxLimit > 0)
                            {
                                if (partnerInvestData.AllPaymentsSum + partSumForPay >= partnerInvestData.MaxLimit)
                                {
                                    partnerInvestData.isCanPay = false;
                                    decimal maxLimitDiff = partnerInvestData.AllPaymentsSum + partSumForPay - partnerInvestData.MaxLimit;
                                    extraSum += maxLimitDiff > partSumForPay ? partSumForPay : maxLimitDiff;
                                    partSumForPay -= maxLimitDiff > partSumForPay ? partSumForPay : maxLimitDiff;

                                    closeInvestment(companyId, item.PartnerId ?? 0, 0, DateTime.Now, user, ds);
                                    isClosePackage = true;
                                }
                                partnerLog += $"Компании:{extraSum.ToString()};Платеж: {partSumForPay.ToString()};\r\n";
                                if (isClosePackage)
                                {
                                    partnerLog += "Закрытие пакета\r\n";
                                    partnerInvestments.Remove(item.PartnerId ?? 0);
                                }
                                //partnerLog += "\r\n";
                                //closeInvestment(companyId, item.PartnerId ?? 0, 0, DateTime.Now, user, ds);
                            }

                            partnerLog += $"---проверка двухкратной выплаты суммы. Всего:{partnerInvestData.AllPaymentsSum.ToString()} Двукратная сумма:{partnerInvestData.DoubleInvestSum.ToString()};";
                            // проверка двухкратной выплаты суммы
                            if (partSumForPay + partnerInvestData.AllPaymentsSum >= partnerInvestData.DoubleInvestSum && !isClosePackage)
                            {
                                partnerInvestData.isCanPay = false;
                                decimal doubleDiffSum = partSumForPay + partnerInvestData.AllPaymentsSum - partnerInvestData.DoubleInvestSum;
                                extraSum += doubleDiffSum > partSumForPay ? partSumForPay : doubleDiffSum;
                                partSumForPay -= doubleDiffSum > partSumForPay ? partSumForPay : doubleDiffSum;
                                partnerLog += $"Компании:{extraSum.ToString()};Платеж: {partSumForPay.ToString()};\r\n";
                                if (!isClosePackage)
                                {
                                    closeInvestment(companyId, item.PartnerId ?? 0, 0, DateTime.Now, user, ds);
                                    partnerLog += "Закрытие пакета\r\n";
                                    isClosePackage = true;
                                    partnerInvestments.Remove(item.PartnerId ?? 0);
                                }
                            }

                            if (extraSum > 0)
                            {
                                ds.PayPayment(ds.CreateInnerTransfer(
                                      accountName: "Остаток.Вознаграждения",
                                      direction: TransferDirection.Input,
                                      companyId: companyId,
                                      partnerId: companyId,
                                      documentId: item.PartnerId ?? 0,
                                      article: "Вознаграждение со структуры",
                                      date: date,
                                      sum: extraSum,
                                      paymentMethod: PaymentMethod.Inner,
                                      comment: $"Вознаграждение со структуры за {ShortName(item)}",
                                      documentType: null,
                                      user: user), date);
                            }

                            if (partSumForPay > 0)
                            {
                                ds.PayPayment(ds.CreateInnerTransfer(
                                      accountName: "Остаток.Вознаграждения",
                                      direction: TransferDirection.Input,
                                      companyId: companyId,
                                      partnerId: item.PartnerId ?? 0,
                                      documentId: item.PartnerId ?? 0,
                                      article: "Вознаграждение со структуры",
                                      date: date,
                                      sum: partSumForPay,
                                      paymentMethod: PaymentMethod.Inner,
                                      comment: $"Вознаграждение со структуры за {ShortName(item)}",
                                      documentType: null,
                                      user: user), date);
                                
                                if (!isClosePackage)
                                {
                                    partnerInvestData.AllPaymentsSum += partSumForPay;
                                    partnerInvestData.DayBinaryPaymentsSum += partSumForPay;
                                    partnerInvestData.MonthBinaryPaymentsSum += partSumForPay;
                                    partnerInvestments[item.PartnerId ?? 0] = partnerInvestData;
                                }
                            }
                            if (partSumForPay > 0 || extraSum > 0)
                            {
                                decimal LeftSum = item.PartnerBinaryLeftShoulderSum ?? 0;
                                //LeftSum -= currentPaymentSum;
                                LeftSum = LeftSum > currentPaymentSum ? LeftSum - currentPaymentSum : 0;
                                decimal RightSum = item.PartnerBinaryRightShoulderSum ?? 0;
                                //RightSum -= currentPaymentSum;
                                RightSum = RightSum > currentPaymentSum ? RightSum - currentPaymentSum : 0;
                                ds.UpdatePartnerBinarySum(LeftSum, RightSum, item.id_object);
                            }
                            //LogPayments(partnerLog);
                        }
                        else
                        {
                            
                            if (partSumForPay > 0)
                            {
                                //extraSum = partSumForPay;
                                ds.PayPayment(ds.CreateInnerTransfer(
                                      accountName: "Остаток.Вознаграждения",
                                      direction: TransferDirection.Input,
                                      companyId: companyId,
                                      partnerId: companyId,
                                      documentId: item.PartnerId ?? 0,
                                      article: "Вознаграждение со структуры",
                                      date: date,
                                      sum: partSumForPay,
                                      paymentMethod: PaymentMethod.Inner,
                                      comment: $"Вознаграждение со структуры за {ShortName(item)}",
                                      documentType: null,
                                      user: user), date);

                                decimal LeftSum = item.PartnerBinaryLeftShoulderSum ?? 0;
                                LeftSum = LeftSum > currentPaymentSum ? LeftSum - currentPaymentSum : 0;
                                decimal RightSum = item.PartnerBinaryRightShoulderSum ?? 0;
                                //RightSum -= currentPaymentSum;
                                RightSum = RightSum > currentPaymentSum ? RightSum - currentPaymentSum : 0;
                                ds.UpdatePartnerBinarySum(LeftSum, RightSum, item.id_object);

                                partnerLog += $"Превышены лимиты, Компании:{partSumForPay.ToString()}\r\n";
                                //LogPayments(partnerLog);
                            }
                        }
                        LogPayments(partnerLog);
                    }
                }


                //old
                /*      var parents = new Dictionary<uint, uint>();
                  var investments = new Dictionary<uint, decimal>();

                  #region Fill Parents And Investments

                  var partners = ds.GetPartners();
                  foreach (var p in partners)
                  {
                      parents.Add(p.id_object, p.InviterId ?? 0);
                      investments.Add(p.id_object, p.BalanceInvestments ?? 0);
                  }
                  #endregion

                  var values = this.CalcInvestValues(parents, investments);
                  var ranks = this.CalcInvestRanks(values);
                  var percents = this.CalcInfinityPercents(parents, ranks);

                  foreach (var payment in expected)
                  {
                      ds.PayPayment(payment.id_object, date);
                      //todo: увеличить выплаченную сумму

                      // если не последний платеж, то

                      #region 10%
                      var i = 0;
                      var parentId = parents[payment.PartnerId.Value];
                      while (parentId > 0)
                      {
                          var levels = this.GetInvestBonusLevels(ranks[parentId]);
                          if (i++ < levels)
                          {
                              ds.PayPayment(ds.CreateInnerTransfer(
                                  accountName: "Остаток.Вознаграждения",
                                  direction: TransferDirection.Input,
                                  companyId: companyId,
                                  partnerId: parentId,
                                  documentId: payment.PartnerId.Value,
                                  article: "Вознаграждение со структуры",
                                  date: date,
                                  sum: payment.PaymentSum.Value * 0.1m,
                                  paymentMethod: PaymentMethod.Inner,
                                  comment: $"Вознаграждение со структуры за {payment.PartnerObjectName}",
                                  documentType: null,
                                  user: user), date);
                          }
                          parentId = parents[parentId];
                      }*/

                #endregion
                #region Бонус бесконечности
                /*var chain = this.GetInfinityPercentChain(parents, percents, payment.PartnerId.Value);
                var transformed = this.TransformInfinityBonusPercent(chain);
                parentId = parents[payment.PartnerId.Value];
                foreach (var p in transformed)
                {
                    if (p > 0)
                    {
                        ds.PayPayment(ds.CreateInnerTransfer(
                            accountName: "Остаток.Вознаграждения",
                            direction: TransferDirection.Input,
                            companyId: companyId,
                            partnerId: parentId,
                            documentId: payment.PartnerId.Value,
                            article: "Вознаграждение со структуры",
                            date: date,
                            sum: payment.PaymentSum.Value * p,
                            paymentMethod: PaymentMethod.Inner,
                            comment: $"Вознаграждение со структуры за {payment.PartnerObjectName}",
                            documentType: null,
                            user: user), date);


                    }
                    parentId = parents[parentId];
                }*/
                #endregion

                // если последний платеж, то проверить есть 
                // ли автопродление. Если оно есть, то
                // создать новый график платежей, а дату 
                // данного платежа перенести на год
                /*     }
                 }*/
            }
        }


        public string ShortName(MarketingPlace place)
        {

            var name = place.ObjectName;
            if (!string.IsNullOrWhiteSpace(place.PartnerFirstName) ||
                !string.IsNullOrWhiteSpace(place.PartnerLastName) ||
                !string.IsNullOrWhiteSpace(place.PartnerMiddleName))
            {
                name = "";
                if (!string.IsNullOrWhiteSpace(place.PartnerLastName))
                    name += place.PartnerLastName + " ";
                if (!string.IsNullOrWhiteSpace(place.PartnerFirstName))
                    name += place.PartnerFirstName.Substring(0, 1) + ".";
                if (!string.IsNullOrWhiteSpace(place.PartnerMiddleName))
                    name += place.PartnerMiddleName.Substring(0, 1) + ".";
            }
            return name;
        }

        public string ShortName(Partner partner)
        {

            var name = "";
            if (!string.IsNullOrWhiteSpace(partner.FirstName) ||
                !string.IsNullOrWhiteSpace(partner.LastName) ||
                !string.IsNullOrWhiteSpace(partner.MiddleName))
            {
                name = "";
                if (!string.IsNullOrWhiteSpace(partner.LastName))
                    name += partner.LastName + " ";
                if (!string.IsNullOrWhiteSpace(partner.FirstName))
                    name += partner.FirstName.Substring(0, 1) + ".";
                if (!string.IsNullOrWhiteSpace(partner.MiddleName))
                    name += partner.MiddleName.Substring(0, 1) + ".";
            }
            return name;
        }

        public PartnerInvestData createPartnerInvestData(uint PartnerId, IDataService ds)
        {
            PartnerInvestData partnerInvestData = new PartnerInvestData();
            var investments = ds.GetInvestments(PartnerId, "Активен");
            decimal MaxLimit = 0;
            decimal allPaymentsSum = 0;
            int investmentsCount = investments.Count();

            /*if (investmentsCount == 0)
            {
                investments = ds.GetInvestments(PartnerId, "Закрыт");
                investmentsCount = investments.Count();
            }*/

            //var partner = ds.GetPartner(PartnerId);
            //partnerInvestData.partner = partner;
            if (investmentsCount > 0)
            {
                var oneInvest = investments.OrderBy(x => x.StartDate).First();
                decimal AllBinaryPayments = CalculateAllBinaryPayments(PartnerId, ds, oneInvest.StartDate ?? DateTime.Now );

                decimal allRefPaymentsSum = CalculateAllPayments(PartnerId, ds,  oneInvest.StartDate ?? DateTime.Now);
                allPaymentsSum += AllBinaryPayments + allRefPaymentsSum;
                var partner = ds.GetPartner(PartnerId);
                var bigInvestment = investments.OrderByDescending(x => x.Sum).First();
                if (bigInvestment.ProgramId > 0)
                {
                    var investProgram = ds.GetInvestProgram(bigInvestment.ProgramId);
                    if (investProgram.MaxLimit > 0)
                    {
                        MaxLimit = (decimal)investProgram.MaxLimit;
                    }
                    partnerInvestData = new PartnerInvestData
                    {
                        Program = investProgram,
                        BalanceInvestments = partner.BalanceInvestments ?? 0,
                        MaxLimit = MaxLimit,
                        AllPaymentsSum = allPaymentsSum,
                        isCanPay = true,
                        AllBinaryPaymentsSum = AllBinaryPayments,
                        partner = partner,
                        DayBinaryPaymentsSum = 0,
                        AllReferalPaymentsSum = allRefPaymentsSum,
                        MonthBinaryPaymentsSum = CalculatePayMonthBinaryPayments(PartnerId, ds),
                        DoubleInvestSum = (decimal)oneInvest.Sum * 2
                    };

                }
            }
            return partnerInvestData;
        }

        public decimal CalculatePayMonthBinaryPayments(uint partnerId, IDataService ds)
        {
            decimal sum = 0;
            string month = DateTime.Now.Month.ToString();
            if(month.Length == 1)
            {
                month = "0" + month;
            }

            int year = DateTime.Now.Year;
            DateTime startMonthDate = DateTime.ParseExact(year + "-" + month + "-01", "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
            var transfers = ds.GetInnerTransfers(partnerId, null, "Вознаграждение со структуры", startMonthDate, DateTime.Now, null, null);
            if (transfers.Count() > 0)
            {
                foreach (var item in transfers)
                {
                    sum += item.PaymentSum ?? 0;
                }
            }
            return sum;
        }

        public decimal CalculateAllBinaryPayments(uint partnerId, IDataService ds,DateTime startDate)
        {
            decimal sum = 0;
            //foreach(var invest in investments) {
                var transfers = ds.GetInnerTransfers(partnerId, null, "Вознаграждение со структуры", startDate, null, null, null);
                if (transfers.Count() > 0)
                {
                    foreach (var item in transfers)
                    {
                        sum += item.PaymentSum ?? 0;
                    }
                }
           // }
            return sum;
        }

        public decimal CalculateAllPayments(uint partnerId, IDataService ds, DateTime startDate)
        {
            decimal sum = 0;
            //foreach (var invest in investments)
           // {
                /*    var transfers = ds.GetInnerTransfers(partnerId, null, "Начисление процентов и тела", startDate, null, null, null);
                if (transfers.Count() > 0)
                {
                    foreach (var item in transfers)
                    {
                        sum += item.PaymentSum ?? 0;
                    }
                }*/
                var transfersRef = ds.GetInnerTransfers(partnerId, null, "Реферальное вознаграждение", startDate, null, null, null);
                if (transfersRef.Count() > 0)
                {
                    foreach (var item in transfersRef)
                    {
                        sum += item.PaymentSum ?? 0;
                    }
                }
            //}
            return sum;
        }

        public void LogPayments(string data, Boolean isRef = false)
        {
            try
            {
                string domain = ConfigurationManager.AppSettings["domain"].ToString();
                mainPath = @"C:\Users\Public\Documents/";
                mainPath += domain;
                
                if (isRef)
                {


                    mainPath += "ref_log.txt";
                }
                else
                {
                    mainPath += "log.txt";
                }

            
                System.IO.StreamWriter sw = new System.IO.StreamWriter(mainPath, true);
                sw.WriteLine(data);
                sw.Close();
            }
            catch (Exception exc)
            {
                return;
            }
        }





        /*
         * Когда создается место под компанией, оно всегда одно
         * 
         * Создание места:
         * Место создается под партнера или без партнера
      
         * 
         *  В принципе для создания места надо знать куда его ставить - 
         *  - вышестоящий
         *  - позиция
         * 
         */


        public bool IsAllowCreateNewPlace(uint marketingId, uint partnerId)
        {
            lock (lockObj)
            {
                IDataService ds = new KbtDataService();
                var placeCount = ds.GetPlaceCount(marketingId, partnerId);
                var referCount = ds.GetReferCount(marketingId, partnerId);
                return this.IsAllowCreateNewPlace(placeCount, referCount);
            }
        }



        /* Если место создается под партнера, то ищется активное место
            * Если место создается без партнера(техн место), то оно встает под первое место
            *
            * Первое место встает под компанию. Под компанией может быть только одно
            * место
            * 
            * Изначально в структуре есть место под компанией
            * 
            * Нужно рассмотреть след сценарии.
            * Место в  - создаются два любых места
            * 
            * 
            */


        public void Test()
        {
            //var ds = new KbtDataService();
            //using (var client = new WebDataClient())
            //{
            //    var query = new QueryItem();
            //    query.AddType("МаркетинговоеМесто", Level.All);
            //    query.AddProperty("СсылкаНаКонтрагента");
            //    var items = client.Search(query).ResultItems();
            //    foreach (var item in items)
            //        if (item.Value<uint>("СсылкаНаКонтрагента") > 0)
            //            ds.UpdatePartnerActivity(item.Value<uint>("СсылкаНаКонтрагента"), true);
            //}

            //using (var client = new WebDataClient())
            //{
            //    var query = new QueryItem();
            //    query.AddType("ОжидаемыйПлатеж");
            //    query.AddProperty("ДатаПлатежа");
            //    query.AddProperty("СтатусПлатежа");
            //    query.AddProperty("СсылкаНаДоговор/ДатаОформления");
            //    query.AddOrder("ДатаПлатежа", Direction.Asc);
            //    var items = client.Search(query).ResultItems();
            //    var i = 0; var count = items.Length;
            //    DateTime date = new DateTime(2018, 12, 14);
            //    foreach (var item in items)
            //    {
            //        var d = item.Value<DateTime>("ДатаПлатежа") - item.Value<DateTime>("СсылкаНаДоговор/ДатаОформления").Date;
            //        client.SetObjectValue(item.Value<uint>("id_object"), "ДатаПлатежа", date.AddDays(d.TotalDays));
            //        i++;
            //    }

            //}

            /*
             * 
             * 
             */

            #region Countries
            //Dictionary<string, string> countries = new Dictionary<string, string>();
            //countries.Add("Абхазия", "Abkhazia");
            //countries.Add("Австралия", "Australia");
            //countries.Add("Австрия", "Austria");
            //countries.Add("Азербайджан", "Azerbaijan");
            //countries.Add("Аландские острова", "Aland Islands");
            //countries.Add("Албания", "Albania");
            //countries.Add("Алжир", "Algeria");
            //countries.Add("Ангилья", "Anguilla");
            //countries.Add("Ангола", "Angola");
            //countries.Add("Андорра", "Andorra");
            //countries.Add("Аргентина", "Argentina");
            //countries.Add("Армения", "Armenia");
            //countries.Add("Аруба", "Aruba");
            //countries.Add("Афганистан", "Afghanistan");
            //countries.Add("Багамские острова", "Bahamas");
            //countries.Add("Бангладеш", "Bangladesh");
            //countries.Add("Барбадос", "Barbados");
            //countries.Add("Бахрейн", "Bahrain");
            //countries.Add("Беларусь", "Belarus");
            //countries.Add("Белиз", "Belize");
            //countries.Add("Бельгия", "Belgium");
            //countries.Add("Бенин", "Benin");
            //countries.Add("Болгария", "Bulgaria");
            //countries.Add("Боливия", "Bolivia");
            //countries.Add("Босния и Герцеговина", "Bosnia & Herzegovina");

            //countries.Add("Ботсвана", "Botswana");
            //countries.Add("Бразилия", "Brazil");
            //countries.Add("Бруней", "Brunei Darussalam");
            //countries.Add("Бурунди", "Burundi");
            //countries.Add("Бутан", "Bhutan");
            //countries.Add("Ватикан", "Vatican City");
            //countries.Add("Великобритания", "United Kingdom");
            //countries.Add("Венгрия", "Hungary");
            //countries.Add("Венесуэла", "Venezuela");
            //countries.Add("Восточный Тимор", "Timor, East");

            //countries.Add("Вьетнам", "Viet Nam");
            //countries.Add("Габон", "Gabon");
            //countries.Add("Гаити", "Haiti");
            //countries.Add("Гамбия", "Gambia");
            //countries.Add("Гана", "Ghana");
            //countries.Add("Гваделупа", "Guadeloupe");
            //countries.Add("Гватемала", "Guatemala");
            //countries.Add("Гвинея", "Guinea");
            //countries.Add("Гвинея-Бисау", "Guinea-Bissau");

            //countries.Add("Германия", "Germany");
            //countries.Add("Гибралтар", "Gibraltar");
            //countries.Add("Гонконг", "Hong Kong");
            //countries.Add("Гондурас", "Honduras");
            //countries.Add("Гренада", "Grenada");
            //countries.Add("Гренландия", "Greenland");
            //countries.Add("Греция", "Greece");
            //countries.Add("Грузия", "Georgia");
            //countries.Add("Гуам", "Guam");
            //countries.Add("Дания", "Denmark");
            //countries.Add("Доминика", "Dominica");
            //countries.Add("Доминиканская Республика", "Dominican Republic");
            //countries.Add("Египет", "Egypt");
            //countries.Add("Замбия", "Zambia");
            //countries.Add("Западная Сахара", "Western Sahara");
            //countries.Add("Зимбабве", "Zimbabwe");
            //countries.Add("Израиль", "Israel");
            //countries.Add("Индия", "India");
            //countries.Add("Индонезия", "Indonesia");
            //countries.Add("Иордания", "Jordan");
            //countries.Add("Ирак", "Iraq");
            //countries.Add("Иран", "Iran");
            //countries.Add("Ирландия", "Ireland");
            //countries.Add("Исландия", "Iceland");
            //countries.Add("Испания", "Spain");
            //countries.Add("Италия", "Italy");
            //countries.Add("Йемен", "Yemen");
            //countries.Add("Казахстан", "Kazakhstan");
            //countries.Add("Камбоджа", "Cambodia");
            //countries.Add("Камерун", "Cameroon");
            //countries.Add("Канада", "Canada");
            //countries.Add("Катар", "Qatar");
            //countries.Add("Кения", "Kenya");
            //countries.Add("Кипр", "Cyprus");
            //countries.Add("Киргизия", "Kyrgyzstan");
            //countries.Add("Кирибати", "Kiribati");
            //countries.Add("Китай", "China");
            //countries.Add("Колумбия", "Colombia");
            //countries.Add("Корея Северная", "Korea, D.P.R.");

            //countries.Add("Корея Южная", "Korea");
            //countries.Add("Коста-Рика", "Costa Rica");
            //countries.Add("Кот-д'Ивуар", "Cote d'Ivoire");

            //countries.Add("Куба", "Cuba");
            //countries.Add("Кувейт", "Kuwait");
            //countries.Add("Лаос", "Lao P.D.R.");
            //countries.Add("Латвия", "Latvia");
            //countries.Add("Лесото", "Lesotho");
            //countries.Add("Либерия", "Liberia");
            //countries.Add("Ливан", "Lebanon");
            //countries.Add("Ливия", "Libyan Arab Jamahiriya");

            //countries.Add("Литва", "Lithuania");
            //countries.Add("Лихтенштейн", "Liechtenstein");
            //countries.Add("Люксембург", "Luxembourg");
            //countries.Add("Маврикий", "Mauritius");
            //countries.Add("Мавритания", "Mauritania");
            //countries.Add("Мадагаскар", "Madagascar");
            //countries.Add("Македония", "Macedonia");
            //countries.Add("Малави", "Malawi");
            //countries.Add("Малайзия", "Malaysia");
            //countries.Add("Мали", "Mali");
            //countries.Add("Мальдивы", "Maldives");
            //countries.Add("Мальта", "Malta");
            //countries.Add("Марокко", "Morocco");
            //countries.Add("Мексика", "Mexico");
            //countries.Add("Мозамбик", "Mozambique");
            //countries.Add("Молдавия", "Moldova");
            //countries.Add("Монако", "Monaco");
            //countries.Add("Монголия", "Mongolia");
            //countries.Add("Намибия", "Namibia");
            //countries.Add("Непал", "Nepal");
            //countries.Add("Нигер", "Niger");
            //countries.Add("Нигерия", "Nigeria");
            //countries.Add("Нидерланды", "Netherlands");
            //countries.Add("Никарагуа", "Nicaragua");
            //countries.Add("Новая Зеландия", "New Zealand");
            //countries.Add("Норвегия", "Norway");
            //countries.Add("ОАЭ", "United Arab Emirates");
            //countries.Add("Оман", "Oman");
            //countries.Add("Пакистан", "Pakistan");
            //countries.Add("Панама", "Panama");
            //countries.Add("Парагвай", "Paraguay");
            //countries.Add("Перу", "Peru");
            //countries.Add("Польша", "Poland");
            //countries.Add("Португалия", "Portugal");
            //countries.Add("Россия", "Russia");
            //countries.Add("Румыния", "Romania");
            //countries.Add("Сан-Марино", "San Marino");
            //countries.Add("Саудовская Аравия", "Saudi Arabia");
            //countries.Add("Сенегал", "Senegal");
            //countries.Add("Сербия", "Serbia");
            //countries.Add("Сингапур", "Singapore");
            //countries.Add("Сирия", "Syrian Arab Republic");
            //countries.Add("Словакия", "Slovakia");
            //countries.Add("Словения", "Slovenia");
            //countries.Add("Сомали", "Somalia");
            //countries.Add("Судан", "Sudan");
            //countries.Add("США", "USA");
            //countries.Add("Таджикистан", "Tajikistan");
            //countries.Add("Таиланд", "Thailand");
            //countries.Add("Танзания", "Tanzania");
            //countries.Add("Того", "Togo");
            //countries.Add("Тунис", "Tunisia");
            //countries.Add("Туркменистан", "Turkmenistan");
            //countries.Add("Турция", "Turkey");
            //countries.Add("Уганда", "Uganda");
            //countries.Add("Узбекистан", "Uzbekistan");
            //countries.Add("Украина", "Ukraine");
            //countries.Add("Уругвай", "Uruguay");
            //countries.Add("Федеративные Штаты Микронезии", "Micronesia");
            //countries.Add("Фиджи", "Fiji");
            //countries.Add("Филиппины", "Philippines");
            //countries.Add("Финляндия", "Finland");
            //countries.Add("Франция", "France");
            //countries.Add("Хорватия", "Croatia");
            //countries.Add("Чад", "Chad");
            //countries.Add("Черногория", "Montenegro");
            //countries.Add("Чехия", "Czech Republic");
            //countries.Add("Чили", "Chile");
            //countries.Add("Швейцария", "Switzerland");
            //countries.Add("Швеция", "Sweden");
            //countries.Add("Шри-Ланка", "Sri Lanka");
            //countries.Add("Эквадор", "Ecuador");
            //countries.Add("Эритрея", "Eritrea");
            //countries.Add("Эстония", "Estonia");
            //countries.Add("Эфиопия", "Ethiopia");
            //countries.Add("ЮАР", "South Africa");
            //countries.Add("Ямайка", "Jamaica");
            //countries.Add("Япония", "Japan");

            //var count = countries.Count;
            //var i = 0;
            //using (var client = new WebDataClient())
            //{
            //    uint dictId = 26120;
            //    foreach (var item in countries)
            //    {
            //        ++i;
            //        var values = new Dictionary<string, object>();
            //        values.Add("Название", item.Value);
            //        values.Add("Значение", item.Key);
            //        values.Add("СсылкаНаСправочник", dictId);
            //        client.InsertObject(dictId, "ЭлементСправочника", values);
            //    }

            //    //i = 0;
            //    //dictId = 53;
            //    //foreach (var item in countries)
            //    //{
            //    //    ++i;
            //    //    var values = new Dictionary<string, object>();
            //    //    values.Add("Название", item.Key);
            //    //    values.Add("Значение", item.Key);
            //    //    values.Add("СсылкаНаСправочник", dictId);
            //    //    client.InsertObject(dictId, "ЭлементСправочника", values);
            //    //}

            //}
            #endregion

            //using (var client = new WebDataClient())
            //{
            //    IDataService ds = new KbtDataService();
            //    var partners = ds.GetPartners().OrderBy(x=>x.CreationDate.Value);
            //    var count = partners.Count();
            //    int i = 100;
            //    foreach (var p in partners)
            //    {
            //        if (p.id_object != 5)
            //        {
            //            client.SetObjectValue(p.id_object, "ПерсональныйНомерКонтрагента", i++);
            //        }
            //    }
            //}


            //using (var client = new WebDataClient())
            //{
            //    var query = new QueryItem();
            //    query.AddType("Платеж", Level.All);
            //    query.AddType("ИнвестиционныйДоговор", Level.All);
            //    query.AddType("Уведомление", Level.All);
            //    query.AddType("МаркетинговоеМесто", Level.All);
            //    query.AddType("Голос", Level.All);



            //    var items = client.Search(query).ResultItems().OrderBy(x => x.Value<string>("hash").Length);
            //    var i = 0;
            //    var count = items.Count();
            //    foreach (var item in items)
            //    {
            //        try
            //        {
            //            client.DeleteObject(item.Value<uint>("id_object"));
            //        }
            //        catch
            //        {
            //            continue;
            //        }
            //        finally
            //        {
            //            i++;
            //        }
            //    }
            //}



            //var ds = new KbtDataService();
            //uint marketingId = 93;
            //var structure = ds.GetStructure(marketingId);
            //DateTime date = DateTime.Now;

            //foreach (var root in structure)
            //{
            //    if (root.id_object == 3344)
            //    {
            //        ;
            //    }

            //    // Проверка накоплений у пригласителя
            //    var referCount = ds.GetReferCount(marketingId, root.PartnerId.Value);
            //    var savings = ds.GetStructSavings(root.id_object);
            //    foreach (var saving in savings)
            //    {
            //        if (this.ChargeOnTheMainWallet(referCount, saving.Level.Value))
            //        {
            //            ds.PayPayment(saving.id_object, date);
            //        }
            //    }
            //}


            //#region в 5 уровней
            ////var ds = new KbtDataService();
            ////uint marketingId = 93;
            ////var structure = ds.GetStructure(marketingId);
            ////var newPlace = ds.GetPlace(newPlaceId);

            ////// начисляем в 5 уровней вверх
            ////// Начисление в пять уровней
            ////structure = ds.GetStructure(marketingId);
            ////var parent = newPlace;
            ////for (var i = 0; i < GetParentCount(newPlace, structure, 5); i++)
            ////{
            ////    parent = this.GetParent(parent, structure, 1);
            ////    var structValue = this.CalcStructValue(parent, structure, 5);

            ////    if (structValue < 58)
            ////    {
            ////        if ((parent.PartnerId ?? 0) > 0)
            ////        {
            ////            // crate id_p

            ////            if (this.ChargeOnTheMainWallet(
            ////                ds.GetReferCount(marketingId, parent.PartnerId.Value),
            ////                i + 1))
            ////            {
            ////                // pay id_p
            ////            }
            ////        }
            ////    }
            ////    else
            ////    {
            ////        if ((parent.PartnerId ?? 0) > 0)
            ////        {
            ////           // repay Parent
            ////        }
            ////        else
            ////        {
            ////            // create 11 tech places


            ////        }
            ////    }
            ////}

            //#endregion
        }


        public int UpdateBalances()
        {
            lock (lockObj)
            {
                int paymentBalanceUpdates = 0;
                int partnerBalanceUpdates = 0;

                Dictionary<string, Dictionary<uint, decimal>> balances = new Dictionary<string, Dictionary<uint, decimal>>();

                using (var client = new WebDataClient())
                {
                    var query = new QueryItem("ВнутреннийПеревод", Level.All, new[] {
                    "СуммаПлатежа", "СуммаПриход", "СуммаРасход", "СуммаОстатокНаСчете", "ДатаСоздания",
                    "НазваниеСчета", "СсылкаНаКонтрагента", "ДатаПлатежа", "НаправлениеПлатежа", "НазваниеСчета"
                });
                    query.AddConditionItem(Union.None, "СтатусПлатежа", Operator.Equal, "Оплачен");
                    query.AddOrder("ДатаПлатежа", Direction.Asc);
                    query.AddOrder("ДатаСоздания", Direction.Asc);
                    var items = client.Search(query).ResultItems();

                    var itemGroups = items.GroupBy(x => x.Value<string>("НазваниеСчета"));

                    foreach (IGrouping<string, JObject> g in itemGroups)
                    {
                        var balance = new Dictionary<uint, decimal>();
                        balances.Add(g.Key, balance);
                        foreach (var item in g)
                        {
                            var partnerId = item.Value<uint>("СсылкаНаКонтрагента");

                            if (!balance.ContainsKey(partnerId))
                                balance.Add(partnerId, 0);

                            var income = item.Value<decimal?>("СуммаПриход") ?? 0m;
                            var outcome = item.Value<decimal?>("СуммаРасход") ?? 0m;
                            var sum = item.Value<decimal?>("СуммаПлатежа") ?? 0m;
                            var direction = item.Value<string>("НаправлениеПлатежа");
                            var b = item.Value<decimal?>("СуммаОстатокНаСчете") ?? 0m;

                            Dictionary<string, object> updates = new Dictionary<string, object>();

                            if (direction == "Приход" && sum != income)
                                updates.Add("СуммаПриход", sum);

                            if (direction == "Приход" && outcome != 0)
                                updates.Add("СуммаРасход", null);

                            if (direction == "Расход" && sum != outcome)
                                updates.Add("СуммаРасход", sum);

                            if (direction == "Расход" && income != 0)
                                updates.Add("СуммаПриход", null);


                            var reminder = balance[partnerId] + income - outcome;

                            if (reminder != b)
                                updates.Add("СуммаОстатокНаСчете", reminder);

                            if (updates.Count > 0)
                            {
                                paymentBalanceUpdates++;
                                client.SetObjectValues(item.Value<uint>("id_object"), updates);
                            }
                            balance[partnerId] = reminder;
                        }
                    }


                    var query3 = new QueryItem();
                    query3.AddType("ЭлементСправочника");
                    query3.AddProperty("Значение");
                    query3.AddConditionItem(Union.None, "СсылкаНаСправочник/Название", Operator.Equal, "Внутренние счета");
                    var accounts = new List<string>();
                    foreach (var acc in client.Search(query3).ResultItems())
                    {
                        accounts.Add(acc.Value<string>("Значение"));
                    }


                    // проверка баланса партнера
                    var query2 = new QueryItem();
                    query2.AddType("Контрагент", Level.All);
                    foreach (string account in accounts)
                    {
                        query2.AddProperty(account);
                    }
                    var items2 = client.Search(query2).ResultItems();
                    foreach (var item in items2)
                    {
                        var partnerId = item.Value<uint>("id_object");
                        var values = new Dictionary<string, object>();
                        foreach (string account in accounts)
                        {


                            //if (partnerId == 235 && item.Value<string>("НазваниеСчета") == "Остаток.Проценты")
                            //{
                            //    ;

                            //}

                            Dictionary<uint, decimal> balance = new Dictionary<uint, decimal>();
                            if (balances.ContainsKey(account))
                                balance = balances[account];
                            var inbase = item.Value<decimal?>(account);

                            if (balance.ContainsKey(partnerId))
                            {
                                if ((inbase ?? 0) != balance[partnerId])
                                    values.Add(account, balance[partnerId]);
                            }
                            else
                            {
                                if (inbase != null)
                                    values.Add(account, null);
                            }
                        }

                        if (values.Count > 0)
                        {
                            partnerBalanceUpdates++;
                            client.SetObjectValues(partnerId, values);
                        }
                    }


                }
                return paymentBalanceUpdates + partnerBalanceUpdates;
            }
        }

        public void ConfirmWithdrawal(uint id_object, string wallet, string tag, string comment, string user)
        {
            // System.IO.File.WriteAllText(@"C:\www\w2w\service.w2w.fund\111.txt", "444");

            lock (lockObj)
            {
             

                BinanceClient.SetDefaultOptions(new BinanceClientOptions()
                {
                    ApiCredentials = new ApiCredentials(
                        key: ConfigurationManager.AppSettings["binance.key"],
                        secret: ConfigurationManager.AppSettings["binance.secret"])
                    /*,LogVerbosity = LogVerbosity.Debug,
                    LogWriters = new List<TextWriter> { Console.Out }*/
                });

               
                IDataService ds = new KbtDataService();
                var request = ds.GetWithdrawalRequest(id_object);
               

                if (request == null)
                {
                    throw new Exception("Заявка не найдена!");
                }


                if (request.ProcessedStatus != "Не обработана")
                {
                    throw new Exception("Заявка уже обработана!");
                }
                

                using (var client = new BinanceClient())
                {
                    var resultSum = request.CurrencySum;

                   

                    // получаем комиссию от бинанс и если она получена, то плюсуем к сумме вывода
                    var comission = client.GetAssetDetails();
                    decimal binance_comission = 0;
                    
                    if (comission.Success)
                    {
                        binance_comission = comission.Data[request.Currency].WithdrawFee;
                        if (request.Currency != "USDT")
                        {
                            resultSum = request.CurrencySum + binance_comission;
                        }
                    }


                    if (!string.IsNullOrWhiteSpace(request.Tag))
                    {
                        var withdraw = client.Withdraw(
                           asset: request.Currency,
                           address: request.WalletAddress,
                           amount: resultSum ?? 0,
                           addressTag: request.Tag);
                    }
                    else
                    {
                        var withdraw = client.Withdraw(
                           asset: request.Currency,
                           address: request.WalletAddress,
                           amount: resultSum ?? 0);
                    }


                    ds.SetWithdrawalDetails(
                        id_object: id_object,
                        processedDate: DateTime.Now,
                        processedStatus: "Обработана",
                        wallet: wallet,
                        destTag: tag,
                        comment: comment,
                        user: user);


                    var id_payment = ds.AddPayment(
                        direction: TransferDirection.Output,
                        companyId: 5,
                        partnerId: request.PartnerId.Value,
                        orderId: id_object,
                        article: "Вывод средств",
                        paymentDate: DateTime.Now,
                        sum: request.PaymentSum.Value,
                        paymentMethod: PaymentMethod.Crypto,
                        rateOfNds: RateOfNDS.БезНДС,
                        desctiption: "Вывод средств",
                        paymentComission: binance_comission,
                        user: user);

                }
            }
        }

        public void CancelWithdrawal(uint id_object, string comment, string user)
        {
            lock (lockObj)
            {
                IDataService ds = new KbtDataService();
                var request = ds.GetWithdrawalRequest(id_object);
                if (request == null)
                    throw new Exception("Заявка не найдена!");

                if (request.ProcessedStatus != "Не обработана")
                    throw new Exception("Заявка уже обработана!");


                ds.SetWithdrawalDetails(
                    id_object: id_object,
                    processedDate: DateTime.Now,
                    processedStatus: "Отменена",
                    wallet: request.WalletAddress,
                    destTag: request.Tag,
                    comment: comment,
                    user: user);


                var id_transfer = ds.CreateInnerTransfer(
                    accountName: request.AccountName,
                    direction: TransferDirection.Input,
                    companyId: 5,
                    partnerId: request.PartnerId.Value,
                    documentId: request.id_object,
                    article: "Отмена вывода средств",
                    date: DateTime.Now,
                    sum: request.PaymentSum.Value,
                    paymentMethod: PaymentMethod.Inner,
                    comment: $"Отмена вывода средств: {comment}",
                    documentType: null,
                    user: user);

                ds.PayPayment(id_transfer, DateTime.Now);
            }
        }

        public void UpdateParentInvestShoulderSum(MarketingPlace place, IEnumerable<MarketingPlace> structure, decimal sum)
        {
            IDataService ds = new KbtDataService();
            var parentPlace = GetParent(place, structure, 1);
            decimal ShoulderSum = 0;
            decimal BinaryShoulderSum = 0;
            bool IsLeftShoulder = true;
            while (parentPlace != null)
            {
                if (parentPlace.PartnerId == 5)
                {
                    break;
                }
                if (place.SortPosition == 0)
                {
                    ShoulderSum = parentPlace.PartnerLeftShoulderInvestSum != null ? (decimal)parentPlace.PartnerLeftShoulderInvestSum : 0;
                    BinaryShoulderSum = parentPlace.PartnerBinaryLeftShoulderSum != null ? (decimal)parentPlace.PartnerBinaryLeftShoulderSum : 0;
                    IsLeftShoulder = true;
                }
                else
                {
                    ShoulderSum = parentPlace.PartnerRightShoulderInvestSum != null ? (decimal)parentPlace.PartnerRightShoulderInvestSum : 0;
                    BinaryShoulderSum = parentPlace.PartnerBinaryRightShoulderSum != null ? (decimal)parentPlace.PartnerBinaryRightShoulderSum : 0;
                    IsLeftShoulder = false;
                }
                ShoulderSum += sum;
                // может быть меньше 0 при закрытии пакета
                if (sum > 0)
                {
                    BinaryShoulderSum += sum * 0.9m;
                }
                ds.UpdatePartnerInvestSum(IsLeftShoulder, ShoulderSum, BinaryShoulderSum, (uint)parentPlace.id_object);
                place = parentPlace;
                parentPlace = GetParent(parentPlace, structure, 1);
            }
        }

        public void closeInvestment(uint companyId,uint partnerId, uint investmentId, DateTime date, string user, IDataService ds)
        {
            var investments = ds.GetInvestments(partnerId, "Активен");
           /* if(investments.Count() == 0)
            {
                investments = ds.GetInvestments(partnerId, "Закрыт");
                if(investments.Count() > 0)
                {
                    foreach(var item in investments)
                    {
                        ds.SetInvestmentStatus(
                           investmentId: item.id_object,
                           status: "Завершен",
                           changeDate: date);
                    }
                }
                return;
            }*/
            var investment = new Investment();
            if (investmentId > 0)
            {
                
                investment = investments.Where(x => x.id_object == investmentId).Single();
            }
            else
            {
                investment = investments.OrderBy(x => x.StartDate).First();
                investmentId = investment.id_object;
            }
            
            TerminateInvestment(companyId, partnerId, investmentId, 0, date, user, investments.Count() == 1);
            ds.AddNotice(partnerId, $"Превышены лимиты по пакету «{investment.ProgramName.Substring(0, 1).ToUpper() + investment.ProgramName.Substring(1)}» от {investment.StartDate.ToString("dd.MM.yyyy")} и он был закрыт");

            var structure = ds.GetStructure(93);
            if (structure.Count() > 0)
            {
                var PartnerPlace = structure.Single(x => x.PartnerId == partnerId);
                if (PartnerPlace != null)
                {
                    UpdateParentInvestShoulderSum(PartnerPlace, structure, (decimal)investment.Sum * -1);
                }
            }

            var partner = ds.GetPartner(partnerId);
            if (partner != null)
            {
                string img = System.Uri.EscapeUriString("https://w2w.fund/content/img/logo-2.png");
                string body = @"<div style='width:786px;'><div style='background:#000; border-radius:10px; padding:10px;'>";
                body += string.Format(@"<img style='height:58px;margin:0 auto; display:block;' src='{0}' /></div>", img);
                body += $"<p><em>Уважаемый(-ая) {partner.FirstName} {partner.LastName}, поздравляем!</em></p>";
                body += $"<p><em>Вы достигли двухкратную доходность по вашему инвестиционному пакету <b>{investment.ProgramName.Substring(0, 1).ToUpper() + investment.ProgramName.Substring(1)}</b> и она была закрыта. <br/>Вы можете приобрести инвестиционный пакет большого номинала для возможности зарабатывать еще больше!</em></p>";
                body += "<p><em>С уважением, Way to Wealth.</em></p></div>";
                SendEmail(partner.Email, "Пакет закрыт", body);
            }
        }

        public void SendEmail(string to, string subject, string body/*uint companyId, string user*/)
        {
            //IDataService ds = new KbtDataService();
            
            string _host = ConfigurationManager.AppSettings["smtphost"];
            string _userName = ConfigurationManager.AppSettings["smtpusername"];
            string _password = ConfigurationManager.AppSettings["smtppassword"];
            bool _useSsl = Convert.ToBoolean(ConfigurationManager.AppSettings["smtpusessl"]);
            int _port = Convert.ToInt32(ConfigurationManager.AppSettings["smtpport"]);
            string verifResult = "Да";
            //string to = "derekspirith@gmail.com";//: this.User.Email,
            //string subject = "test";
            //string img = System.Uri.EscapeUriString("https://w2w.fund/content/img/logo.png");
            /*string body = @"<div style='width:786px;'><div style='background:#707070; border-radius:10px; padding:10px;'>";
            body += string.Format(@"<img style='height:58px;margin:0 auto; display:block;' src='{0}' /></div>", img);
            body += @"<p><em>Уважаемый пользователь,</em></p>";
            body += @"<p><em>Ваши документы " + verifResult + " верификацию.</em></p>";
            body += @"<p><em>С уважением, команда платформы Way to Wealth.</em></p></div>"; ;// string.Format(Resources.Resource.LetterFinancialPassword, tp.Password));*/
            var message = new MimeMessage();
            message.From.Add(new MailboxAddress(_userName));
            message.To.Add(new MailboxAddress(to));
            message.Subject = subject;

            //var bodyBuilder = new BodyBuilder { TextBody = body };
            var bodyBuilder = new BodyBuilder { HtmlBody = body };
            message.Body = bodyBuilder.ToMessageBody();
            using (var client = new MailKit.Net.Smtp.SmtpClient())
            {
                client.ServerCertificateValidationCallback = (s, c, h, e) => true;
                client.Connect(_host, _port, _useSsl);
                client.AuthenticationMechanisms.Remove("XOAUTH2");
                client.Authenticate(_userName, _password);
                client.Send(message);
                client.Disconnect(true);
            }
        }
    }
}
