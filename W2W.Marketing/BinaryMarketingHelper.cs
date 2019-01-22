using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace W2W.Marketing
{
    public class BinaryMarketingHelper
    {
        public string GetNewPlace(uint rootId)
        {
            throw new NotImplementedException();
        }
        public uint GetRootId(uint marketingId, uint partnerId)
        {
            throw new NotImplementedException();
        }

        /*
         * Нужно определить ParentHash
         *
         * 
         */


        public string GetActiveHashCode(IEnumerable<string> partnerHashCodes, IEnumerable<string> hashCodes)
        {
            IOrderedEnumerable<string> source = from x in partnerHashCodes
                                                orderby x.Length
                                                select x;
            for (int i = 0; i < source.Count(); i++)
            {
                if (!IsFilled(source.ElementAt(i), hashCodes, 5))
                {
                    return source.ElementAt(i);
                }
            }
            return source.First();
        }


        /*
         * 2 ситуации:
         * - partnerId указан
         *   - компания ("1")
         *   - не компания (!"1")
         *   
         * - partnerId Не указан
         *   - только место компании, тогда как обычно
         *   - не только место компании, тогда 
         *   
         * 
         * 
         * 
         */ 














        #region Базовые
        // Возвращает всех родителей в levels уровнях
        public IEnumerable<string> GetParents(string hashCode, int levels)
        {
            while (hashCode.Length > 1)
            {
                int num = levels;
                levels = num - 1;
                if (num <= 0)
                {
                    break;
                }
                hashCode = hashCode.Substring(0, hashCode.Length - 1);
                yield return hashCode;
            }
        }

        // Вовзаращает количество элементов в уровнях levels для hashCode
        public int CalcStructValue(string hashCode, IEnumerable<string> hashCodes,
            int levels)
        {
            int maxlength = hashCode.Length + levels;
            return hashCodes.Count(x =>
                x.StartsWith(hashCode) &&
                x.Length <= maxlength &&
                x != hashCode);
        }

        // Определяет заполнены ли уровни
        public bool IsFilled(string baseHashCode, IEnumerable<string> hashCodes, int levels)
        {
            for (int i = 1; i <= levels; i++)
            {
                if (!IsLevelFilled(baseHashCode, i, hashCodes))
                {
                    return false;
                }
            }
            return true;
        }

        // определяет заполнен ли уровень
        public bool IsLevelFilled(string baseHashCode, int level, IEnumerable<string> hashCodes)
        {
            foreach (string levelHashCode in GetLevelHashCodes(baseHashCode, level))
            {
                if (!hashCodes.Contains(levelHashCode))
                {
                    return false;
                }
            }
            return true;
        }

        // tested
        // Возвращает все возможные хэши на уровне Level Для baseHashCode
        public IEnumerable<string> GetLevelHashCodes(string baseHashCode, int level)
        {
            decimal count = Convert.ToDecimal(Math.Pow(2.0, level));
            for (int i = 0; (decimal)i < count; i++)
            {
                string str = Convert.ToString(i, 2).PadLeft(level, '0');
                yield return baseHashCode + str;
            }
        }

        // tested
        // Возвращает родителя на определенном уровне
        public string GetParent(string hashCode, int level)
        {
            if (level >= hashCode.Length)
            {
                return null;
            }
            return hashCode.Substring(0, hashCode.Length - level);
        }

        // tested
        // Определяет количество родителей, но не более max
        public int GetParentCount(string hashcode, int max)
        {
            return Math.Min(max, hashcode.Length - 1);
        }

        // Возвращает свободный Хэш код у родителя
        public string GetFreeHashCode(string roothash, IEnumerable<string> hashCodes)
        {
            if (string.IsNullOrWhiteSpace(roothash))
            {
                return "1";
            }
            string nextHashCode = GetNextHashCode(roothash, roothash);
            while (!IsHashCodeFree(nextHashCode, hashCodes))
            {
                nextHashCode = GetNextHashCode(roothash, nextHashCode);
            }
            return nextHashCode;
        }

        // Возвращает следующий по очереди хэш код (слева направо)
        public string GetNextHashCode(string basichash, string prevhash)
        {
            int deep = prevhash.Length - basichash.Length; // глубина относит корня
            int basic = Convert.ToInt32(basichash, 2); // в десятичном выражении
            int prev = Convert.ToInt32(prevhash, 2); // в десятичном выражении
            int count = Convert.ToInt32(Math.Pow(2.0, deep)); // макс кол-во элементов на Глубине deep
            var first = count * basic; // первый на уровне deep элемент
            int offset = prev - first; // Отклоненние относительно first
            
            /* 
             * Если мы дошли до последнего элемента - переход на след уровень,
             * иначе +1
             */

            int next = (offset >= count - 1) ? (2 * count * basic) : (prev + 1); 
            return Convert.ToString(next, 2);
        }

        // Определяет свободен ли Хэш
        public bool IsHashCodeFree(string hashCode, IEnumerable<string> hashCodes)
        {
            if (hashCodes != null)
            {
                return !hashCodes.Contains(hashCode);
            }
            return true;
        }
        
        // tested
        // Определяет глубину места в структуре
        public int GetDeepByHashCode(string hashCode)
        {
            return hashCode.Length - 1;
        }

        // tested
        // Определяет позицию места
        public int GetPosByHashCode(string hashCode)
        {
            if (!hashCode.EndsWith("0"))
            {
                return 1;
            }
            return 0;
        }

        #endregion
    }
}