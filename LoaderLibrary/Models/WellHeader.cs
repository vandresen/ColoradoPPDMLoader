namespace LoaderLibrary.Models
{
    public class WellHeader
    {
        public string UWI { get; set; }
        public string WELL_NAME { get; set; }
        public double? SURFACE_LONGITUDE { get; set; }
        public double? SURFACE_LATITUDE { get; set; }
        public double? BOTTOM_HOLE_LATITUDE { get; set; }
        public double? BOTTOM_HOLE_LONGITUDE { get; set; }
        public string LEASE_NAME { get; set; }
        public string CURRENT_STATUS { get; set; }
        public DateTime? CURRENT_STATUS_DATE { get; set; }
        public string CURRENT_CLASS { get; set; }
        public string OPERATOR { get; set; }
        public string ASSIGNED_FIELD { get; set; }
        public string WELL_NUM { get; set; }
        public double? FINAL_TD { get; set; }
        public double? DEPTH_DATUM_ELEV { get; set; }
        public string DEPTH_DATUM { get; set; }
        public string REMARK { get; set; }
        public DateTime? COMPLETION_DATE { get; set; }
        public DateTime? SPUD_DATE { get; set; }
    }
}
