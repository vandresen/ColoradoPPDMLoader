using LoaderLibrary.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoaderLibrary.Extensions
{
    public static class CommonExtensions
    {
        public static List<ReferenceData> CreateReferenceDataObject(this List<string> refValues)
        {
            List<ReferenceData> refs = new List<ReferenceData>();
            foreach (var value in refValues)
            {
                ReferenceData refData = new ReferenceData() { Reference = value };
                refs.Add(refData);
            }
            return refs;
        }
    }
}
