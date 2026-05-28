import 'dart:convert';

import 'package:crypto/crypto.dart';

/// Extension methods for the [Uri] class, providing additional utilities.
extension UriExt on Uri {
  /// Returns a string representation of the URI with a truncated path.
  ///
  /// [relativePath] specifies how many segments from the end of the path should be excluded.
  /// For example, if the URI is `https://example.com/a/b/c` and [relativePath] is 1,
  /// the result will be `https://example.com/a/b`.
  /// Throws an [Exception] if the path segments are empty.
  String pathPrefix([int relativePath = 0]) {
    if (pathSegments.isEmpty) throw Exception("Path segments are empty");
    // Remove the last [relativePath] + 1 segments from the path.
    List<String> segments =
        pathSegments.sublist(0, pathSegments.length - 1 - relativePath);
    // Create a new URI with the truncated path and no query parameters.
    Uri newUri = replace(pathSegments: segments, queryParameters: {});
    // Return the string representation without the query part.
    return newUri.toString().replaceAll('?', '');
  }

  /// Returns the base URL of the URI, consisting of the scheme, host, and port (if present).
  String get base {
    return this.scheme +
        '://' +
        this.host +
        (this.hasPort ? ':${this.port}' : '');
  }

  /// Generates the MD5 hash of the URI as a string.
  ///
  /// Converts the URI to a string, encodes it as UTF-8, and returns the MD5 hash.
  String get generateMd5 {
    if (this.scheme.toLowerCase().startsWith('http') && this.host != '127.0.0.1' && this.host != 'localhost') {
      // 🚀 核心可用性秒开优化：仅对外部网络请求 URL 去除 Query 参数生成缓存目录 MD5，
      // 确保带动态时间戳/预签名授权参数（如 S3 Signature）的 URL 能够共享同个物理磁盘缓存文件夹！
      // ⚠️ 必须保留本地环回代理（127.0.0.1/localhost）的 queryParameters，否则 startRange/endRange 分片索引信息会被误删导致分片卡死冲突！
      return md5.convert(utf8.encode(this.replace(queryParameters: {}).toString())).toString();
    }
    return md5.convert(utf8.encode(this.toString())).toString();
  }
}
