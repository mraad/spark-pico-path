import arcpy
import os
import re


class Toolbox(object):
    def __init__(self):
        self.label = "ImportWKTToolbox"
        self.alias = "ImportWKTToolbox"
        self.tools = [ImportWKTTool]


class ImportWKTTool(object):
    def __init__(self):
        self.label = "Import WKT"
        self.description = "Import WKT"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(
            name="out_fc",
            displayName="out_fc",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        param_file = arcpy.Parameter(
            name="in_file",
            displayName="Input WKT Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        param_file.value = os.environ["__PYT_FILE__"] if "__PYT_FILE__" in os.environ else "Z:\\Share\\"

        param_name = arcpy.Parameter(
            name="in_name",
            displayName="Layer Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param_name.value = ""

        param_sp_ref = arcpy.Parameter(
            name="in_sp_ref",
            displayName="Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        # Get default SRID from env var.
        sr_id = int(os.environ["__PYT_SRID__"]) if "__PYT_SRID__" in os.environ else 4326
        param_sp_ref.value = arcpy.SpatialReference(sr_id).exportToString()

        param_fields = arcpy.Parameter(name="in_fields",
                                       displayName="Fields",
                                       direction="Input",
                                       datatype="GPString",
                                       parameterType="Optional")
        param_fields.value = ""

        return [param_fc, param_file, param_name, param_sp_ref, param_fields]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        file_name = parameters[1].valueAsText
        if file_name and not parameters[2].value:
            name, _ = os.path.splitext(os.path.basename(file_name))
            parameters[2].value = name

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        p_file = parameters[1].valueAsText
        p_name = parameters[2].value
        p_sp_ref = parameters[3].value
        p_fields = parameters[4].value

        # Save the last file value as an env var.
        # Do not do that if you publish this GP Task to server.
        # This is for desktop mode only so we do not have to retype values.
        os.environ["__PYT_FILE__"] = p_file

        fc = os.path.join(arcpy.env.scratchGDB, p_name)
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        f_type = "POINT"
        # Open file and read first line to determine the shape type.
        with open(p_file, "r") as f:
            line = f.readline()
            wkt = line.split("\t")[-1]
            if "LINESTRING" in wkt:
                f_type = "POLYLINE"
            elif "POLYGON" in wkt:
                f_type = "POLYGON"
            elif "MULTIPOINT" in wkt:
                f_type = "MULTIPOINT"

        arcpy.AddMessage("Setting feature class type to {}".format(f_type))

        field_names = []
        field_indexes = []

        p0 = re.compile("([a-zA-Z]\w*):(\d+):([A-Z]+)")
        p1 = re.compile("([a-zA-Z]\w*):(\d+)")
        p2 = re.compile("([a-zA-Z]\w*):([A-Z]+)")
        p3 = re.compile("([a-zA-Z]\w*):([A-Z]+):(\d+)")

        arcpy.management.CreateFeatureclass(
            arcpy.env.scratchGDB,
            p_name,
            f_type,
            spatial_reference=p_sp_ref,
            has_m="DISABLED",
            has_z="DISABLED")
        if p_fields:
            for index, field in enumerate(p_fields.split(",")):
                m = p0.match(field)
                if m:
                    arcpy.management.AddField(fc, m.group(1), m.group(3), field_length=128)
                    field_names.append(m.group(1))
                    field_indexes.append(int(m.group(2)))
                    continue
                m = p1.match(field)
                if m:
                    arcpy.management.AddField(fc, m.group(1), "TEXT", field_length=128)
                    field_names.append(m.group(1))
                    field_indexes.append(int(m.group(2)))
                    continue
                m = p2.match(field)
                if m:
                    arcpy.management.AddField(fc, m.group(1), m.group(2), field_length=128)
                    field_names.append(m.group(1))
                    field_indexes.append(index)
                    continue
                m = p3.match(field)
                if m:
                    arcpy.management.AddField(fc, m.group(1), m.group(2), field_length=128)
                    field_names.append(m.group(1))
                    field_indexes.append(int(m.group(3)))
                    continue
                arcpy.management.AddField(fc, field, "TEXT", field_length=128)
                field_names.append(field)
                field_indexes.append(index)

        field_names.append("SHAPE@WKT")
        field_indexes.append(-1)

        with arcpy.da.InsertCursor(fc, field_names) as cursor:
            with open(p_file, "r") as f:
                for line in f:
                    t = line.rstrip("\n").split("\t")
                    row = []
                    for idx in field_indexes:
                        row.append(t[idx])
                    try:
                        cursor.insertRow(row)
                    except Exception as e:
                        arcpy.AddWarning(line[:32])
                        arcpy.AddWarning(str(e))

        # Check if symbology file exists. If so, apply it.
        if "__PYT_SYMB__" in os.environ:
            symbology = os.path.join(os.environ["__PYT_SYMB__"], p_name + ".lyrx")
            if os.path.exists(symbology):
                parameters[0].symbology = symbology
        parameters[0].value = fc
