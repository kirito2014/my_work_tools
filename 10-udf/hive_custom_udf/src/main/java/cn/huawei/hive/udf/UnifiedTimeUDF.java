package cn.huawei.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Date;
import java.util.Locale;

public class UnifiedTimeUDF extends UDF {

    public String evaluate(Object objDate) throws Exception {
        //any 日期格式
        String strDate = String.valueOf(objDate);
        String unionDate;

        // 各日期格式 匹配各自的处理方式
        //统一的正则匹配(strDate, 1), 匹配日期格式,  数字表示不同的正则规则
        if (regexDate(strDate, 1)) {
            //日期格式：ddMMMyy  23Apr23
            unionDate = strDate;
        } else if (regexDate(strDate, 2)) {
            //yyyy-MM-dd、yyyy-M-dd、yyyy-MM-d、yyyy-M-d
            unionDate = parseDate(strDate, "yyyy-M-d");
        } else if (regexDate(strDate, 3)) {
            // uuuu-MM-dd HH:mm:ss
            unionDate = parseTimestamp(strDate, "uuuu-MM-dd HH:mm:ss");
        } else if (regexDate(strDate, 4)) {
            // uuuu-MM-dd-HH.mm.ss.SSSSSS
            unionDate = parseTimestamp(strDate, "uuuu-MM-dd-HH.mm.ss.SSSSSS");
        } else if (regexDate(strDate, 5)) {
            // yyyy/MM/dd、yyyy/MM/d、yyyy/M/dd、yyyy/M/d
            unionDate = parseDate(strDate, "yyyy/M/d");
        } else if (regexDate(strDate, 6)) {
            // uuuuMMdd HH:mm:ss
            unionDate = parseTimestamp(strDate, "uuuuMMdd HH:mm:ss");
        } else if (regexDate(strDate, 7)) {
            // uuuuMMdd HHmmss
            unionDate = parseTimestamp(strDate, "uuuuMMdd HHmmss");
        } else if (regexDate(strDate, 8)) {
            // 秒级时间戳 与 毫秒级时间戳
            long timestamp;
            if (10 == strDate.length()) {
                timestamp = Long.parseLong(strDate) * 1000;
            } else {
                timestamp = Long.parseLong(strDate);
            }
            Date tsDate = new Date(timestamp);
            SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            unionDate = parseTimestamp(tsFormat.format(tsDate), "uuuu-MM-dd HH:mm:ss.SSS");
        } else if (regexDate(strDate, 9)) {
            // uuuuMMddHHmmss
            unionDate = parseTimestamp(strDate, "uuuuMMddHHmmss");
        } else if (regexDate(strDate, 10)) {
            // uuuuMMddHHmmss.SSSSS
            unionDate = parseTimestamp(strDate, "uuuuMMddHHmmss.SSSSS");
        } else if (regexDate(strDate, 11)) {
            // yyyyMMdd
            unionDate = parseDate(strDate, "yyyyMMdd");
        } else {
            // NULL值/空值/其他非法日期//其他格式日期
            if (objDate == null || "null".equals(strDate) || "NULL".equals(strDate)) {
                return null;
            } else if ("".equals(strDate) || "0".equals(strDate) ) {
                unionDate =  "1899-12-31";
            } else {
                //没匹配到的日期格式原样输出
                return strDate;
            }
        }
        return unionDate;
    }

    public static String parseDate(String strDate, String dateFormat) {
        Date parsedDate;
        SimpleDateFormat inputFormat;
        try {
            inputFormat = new SimpleDateFormat(dateFormat, Locale.ENGLISH);
            inputFormat.setLenient(false);
            parsedDate = inputFormat.parse(strDate);
        } catch (ParseException e) {
            return strDate;
        }
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
        return outputFormat.format(parsedDate);
    }

    public static String parseTimestamp(String strDate, String dateFormat) {
        LocalDateTime timestamp;
        DateTimeFormatter timeStampFormatter = DateTimeFormatter.ofPattern(dateFormat, Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);
        try {
            timestamp = LocalDateTime.parse(strDate, timeStampFormatter);
        } catch (DateTimeParseException e) {
            return strDate;
        }
        DateTimeFormatter unionTimestampFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS", Locale.ENGLISH);
        return timestamp.format(unionTimestampFormatter);
    }


    public boolean regexDate(String strDate, Integer dateFormat) {
        Boolean isMatch = false;
        switch (dateFormat) {
            case 1:
                isMatch = strDate.matches("\\d{2}[A-Za-z]{3}\\d{2}");
                break;
            case 2:
                isMatch = strDate.matches("\\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12]\\d|3[01])");
                break;
            case 3:
                isMatch = strDate.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
                break;
            case 4:
                isMatch = strDate.matches("\\d{4}-\\d{2}-\\d{2}-\\d{2}\\.\\d{2}\\.\\d{2}\\.\\d{6}");
                break;
            case 5:
                isMatch = strDate.matches("\\d{4}/\\d{1,2}/\\d{1,2}");
                break;
            case 6:
                isMatch = strDate.matches("\\d{8} \\d{2}:\\d{2}:\\d{2}");
                break;
            case 7:
                isMatch = strDate.matches("\\d{8} \\d{6}");
                break;
            case 8:
                isMatch = strDate.matches("\\d{10}|\\d{13}");
                break;
            case 9:
                isMatch = strDate.matches("\\d{14}");
                break;
            case 10:
                isMatch = strDate.matches("\\d{14}\\.\\d{5}");
                break;
            case 11:
                isMatch = strDate.matches("\\d{8}");
                break;
            default:
                break;
        }
        return isMatch;

    }

}
