import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dio/dio.dart';

import '../cache/lru_cache_singleton.dart';
import '../download/download_status.dart';
import '../download/download_task.dart';
import '../ext/file_ext.dart';
import '../ext/int_ext.dart';
import '../ext/log_ext.dart';
import '../ext/socket_ext.dart';
import '../ext/string_ext.dart';
import '../ext/uri_ext.dart';
import '../global/config.dart';
import '../proxy/video_proxy.dart';
import 'url_parser.dart';

/// MP4 URL parser implementation.
/// Handles caching, downloading, and parsing of MP4 video files.
class UrlParserMp4 implements UrlParser {
  DownloadTask _contentLengthTask(Uri uri, Map<String, String> headers) {
    // 🚀 核心秒开与跨页缓存共享优化：净化 uri 的 queryParameters 仅保留核心路径作为 .meta 文件的唯一缓存键！
    // 彻底防止因认证 Token / S3 签名失效或动态变化导致 .meta 文件发生 Cache Miss，
    // 从而强迫 proxy 联网发送超慢且易挂起的 HEAD 请求以读取文件长度，消除切换页面时的 Loading 闪烁与挂起！
    final cleanUri = uri.replace(queryParameters: {});
    return DownloadTask(
      uri: uri,
      fileName: '${cleanUri.toString()}#content_length',
      startRange: 0,
      endRange: null,
      headers: headers,
    );
  }

  int _parseTotalLengthFromHeaders(Headers headers) {
    final contentRange = headers.value(HttpHeaders.contentRangeHeader);
    if (contentRange != null) {
      final match = RegExp(r'bytes (\d+)-(\d+)/(\d+)').firstMatch(contentRange);
      final total = match?.group(3);
      if (total != null && total.isNotEmpty && total != '0') {
        return int.tryParse(total) ?? -1;
      }
    }
    final contentLength = headers.value(HttpHeaders.contentLengthHeader);
    return int.tryParse(contentLength ?? '-1') ?? -1;
  }

  /// Finds the content length of the video locally by scanning its cache directory.
  Future<int> _findContentLengthLocally(Uri uri) async {
    try {
      final cacheKey = uri.generateMd5;
      final cachePath = await FileExt.createCachePath(cacheKey);
      final dir = Directory(cachePath);
      if (await dir.exists()) {
        final files = dir.listSync();
        for (var file in files) {
          if (file is File && file.path.endsWith('.meta')) {
            final content = await file.readAsString();
            final len = int.tryParse(content.trim()) ?? 0;
            if (len > 0) {
              logD('[UrlParserMp4] Found cached content length from local meta file: $len');
              return len;
            }
          }
        }
      }
    } catch (e) {
      logE('[UrlParserMp4] Error finding content length locally: $e');
    }
    return 0;
  }
  /// Retrieves cached data for the given [task] from memory or file.
  ///
  /// Returns a [Uint8List] containing the cached data if available,
  /// or `null` if the data is not cached.
  @override
  Future<Uint8List?> cache(DownloadTask task) async {
    Uint8List? dataMemory = await LruCacheSingleton().memoryGet(task.matchUrl);
    if (dataMemory != null) {
      logD(
        'From memory: ${dataMemory.lengthInBytes.toMemorySize}, '
        'total memory size: ${await LruCacheSingleton().memoryFormatSize()}'
        'Request range：${task.startRange}-${task.endRange}',
      );
      return dataMemory;
    }
    Uint8List? dataFile = await LruCacheSingleton().storageGet(task.matchUrl);
    if (dataFile != null) {
      logD(
        'From file: ${task.matchUrl} '
        'Request range：${task.startRange}-${task.endRange}',
      );
      await LruCacheSingleton().memoryPut(task.matchUrl, dataFile);
      return dataFile;
    }
    return null;
  }

  /// Downloads data from the network for the given [task].
  ///
  /// Returns a [Uint8List] containing the downloaded data,
  /// or `null` if the download fails.
  @override
  Future<Uint8List?> download(DownloadTask task) async {
    logD('From network: ${task.url}');
    Uint8List? dataNetwork;
    task.cacheDir = await FileExt.createCachePath(task.uri.generateMd5);
    await VideoProxy.downloadManager.executeTask(task);
    await for (DownloadTask taskStream in VideoProxy.downloadManager.stream) {
      if (taskStream.matchUrl == task.matchUrl) {
        if (taskStream.status == DownloadStatus.COMPLETED) {
          dataNetwork = Uint8List.fromList(taskStream.data);
          break;
        } else if (taskStream.status == DownloadStatus.FAILED ||
            taskStream.status == DownloadStatus.CANCELLED) {
          logW('[UrlParserMp4] Download failed or cancelled for ${task.url}');
          break;
        }
      }
    }
    return dataNetwork;
  }

  /// Parses the request and returns the data to the [socket].
  ///
  /// Handles HTTP range requests for large file downloads, splitting the file
  /// into segments (default 2MB, configurable via [Config.segmentSize]).
  ///
  /// Returns `true` if parsing and response succeed, otherwise `false`.
  @override
  Future<bool> parse(
    Socket socket,
    Uri uri,
    Map<String, String> headers,
  ) async {
    try {
      // Implementation for parsing and responding to HTTP range requests.
      // Handles both Android and iOS platforms.
      RegExp exp = RegExp(r'bytes=(\d+)-(\d*)');
      RegExpMatch? rangeMatch = exp.firstMatch(headers['range'] ?? '');
      int requestRangeStart = int.tryParse(rangeMatch?.group(1) ?? '0') ?? 0;
      int requestRangeEnd = int.tryParse(rangeMatch?.group(2) ?? '0') ?? -1;
      bool partial = headers.containsKey('range') || headers.containsKey('Range') || requestRangeStart > 0 || requestRangeEnd > 0;
      List<String> responseHeaders = <String>[
        partial ? 'HTTP/1.1 206 Partial Content' : 'HTTP/1.1 200 OK',
        'Accept-Ranges: bytes',
        'Content-Type: video/mp4',
      ];

      if (Platform.isAndroid) {
        await parseAndroid(
          socket,
          uri,
          responseHeaders,
          requestRangeStart,
          requestRangeEnd,
          headers,
        );
      } else {
        await parseIOS(
          socket,
          uri,
          responseHeaders,
          requestRangeStart,
          requestRangeEnd,
          headers,
        );
      }
      await socket.flush();
      return true;
    } catch (e) {
      // Handles any errors during parsing.
      logW('[UrlParserMp4] ⚠ ⚠ ⚠ parse error: $e');
      return false;
    } finally {
      // Ensures the socket is closed after processing.
      await socket.close();
      logD('Connection closed\n');
    }
  }

  /// Parses and responds to range requests on Android.
  ///
  /// Handles segmented download and response for large files.
  Future<void> parseAndroid(
    Socket socket,
    Uri uri,
    List<String> responseHeaders,
    int requestRangeStart,
    int requestRangeEnd,
    Map<String, String> headers,
  ) async {
    final infoTask = _contentLengthTask(uri, headers);
    Uint8List? data = await cache(infoTask);
    int contentLength = 0;
    if (data != null) {
      contentLength = int.tryParse(Utf8Codec().decode(data)) ?? 0;
    }
    if (contentLength <= 0) {
      contentLength = await _findContentLengthLocally(uri);
    }
    if (contentLength <= 0) {
      try {
        contentLength = await head(uri, headers: headers);
        if (contentLength > 0) {
          await _cacheContentLength(infoTask, contentLength);
        }
      } catch (e) {
        logE('[UrlParserMp4] Get content length online failed in parseAndroid: $e');
      }
    }

    if (contentLength <= 0) {
      logE('[UrlParserMp4] CRITICAL: Cannot determine content length offline in parseAndroid, aborting.');
      await socket.close();
      return;
    }

    requestRangeEnd = contentLength - 1;
    responseHeaders.add('Content-Length: ${contentLength - requestRangeStart}');
    responseHeaders.add(
      'Content-Range: bytes '
      '$requestRangeStart-$requestRangeEnd/$contentLength',
    );
    await socket.append(responseHeaders.join('\r\n'));

    bool downloading = true;
    int startRange =
        requestRangeStart - (requestRangeStart % Config.segmentSize);
    int endRange = startRange + Config.segmentSize - 1;
    int retry = 3;
    while (downloading) {
      DownloadTask task = DownloadTask(
        uri: uri,
        startRange: startRange,
        endRange: endRange,
        headers: headers,
      );
      logD(
        'Start ${task.url} '
        'Request range：${task.startRange}-${task.endRange}',
      );

      Uint8List? data = await cache(task);
      // if the task has been added, wait for the download to complete
      bool exitUri = VideoProxy.downloadManager.isTaskExit(task);
      if (exitUri) {
        while (data == null) {
          await Future.delayed(const Duration(milliseconds: 100));
          data = await cache(task);
        }
      }
      if (data == null) {
        concurrent(task, headers);
        task.priority += 2;
        data = await download(task);
      }
      if (data == null) {
        retry--;
        if (retry == 0) {
          downloading = false;
          break;
        }
        continue;
      }

      int startIndex = 0;
      int? endIndex;
      if (startRange < requestRangeStart) {
        startIndex = requestRangeStart - startRange;
      }
      if (endRange > requestRangeEnd) {
        endIndex = requestRangeEnd - startRange + 1;
      }
      data = data.sublist(startIndex, endIndex);
      socket.done.then((value) {
        downloading = false;
      }).catchError((e) {
        downloading = false;
      });
      bool success = await socket.append(data);
      if (!success) downloading = false;
      startRange += Config.segmentSize;
      endRange = startRange + Config.segmentSize - 1;
      if (startRange > requestRangeEnd) {
        downloading = false;
      }
    }
  }

  /// Parses and responds to range requests on iOS.
  ///
  /// Handles segmented download and response for large files.
  Future<void> parseIOS(
    Socket socket,
    Uri uri,
    List<String> responseHeaders,
    int requestRangeStart,
    int requestRangeEnd,
    Map<String, String> headers,
  ) async {
    final infoTask = _contentLengthTask(uri, headers);
    Uint8List? infoData = await cache(infoTask);
    int totalContentLength = 0;
    if (infoData != null) {
      totalContentLength = int.tryParse(Utf8Codec().decode(infoData)) ?? 0;
    }
    if (totalContentLength <= 0) {
      totalContentLength = await _findContentLengthLocally(uri);
    }
    if (totalContentLength <= 0) {
      try {
        totalContentLength = await head(uri, headers: headers);
        if (totalContentLength > 0) {
          await _cacheContentLength(infoTask, totalContentLength);
        }
      } catch (e) {
        logE('[UrlParserMp4] Get content length online failed (offline?): $e');
      }
    }

    if (totalContentLength <= 0) {
      logE('[UrlParserMp4] CRITICAL: Cannot determine content length offline in parseIOS, aborting.');
      await socket.close();
      return;
    }

    if (requestRangeStart == 0 && requestRangeEnd == 1) {
      responseHeaders.add('Content-Range: bytes 0-1/$totalContentLength');
      await socket.append(responseHeaders.join('\r\n'));
      await socket.append([0]);
      await socket.close();
      return;
    }

    if (requestRangeEnd == -1) {
      requestRangeEnd = totalContentLength - 1;
    }

    int contentLength = requestRangeEnd - requestRangeStart + 1;
    responseHeaders.add('Content-Length: $contentLength');
    if (totalContentLength > 0) {
      responseHeaders.add(
        'Content-Range: bytes $requestRangeStart-$requestRangeEnd/$totalContentLength',
      );
    }
    await socket.append(responseHeaders.join('\r\n'));

    bool downloading = true;
    int startRange =
        requestRangeStart - (requestRangeStart % Config.segmentSize);
    int endRange = startRange + Config.segmentSize - 1;
    int retry = 3;
    while (downloading) {
      DownloadTask task = DownloadTask(
        uri: uri,
        startRange: startRange,
        endRange: endRange,
        headers: headers,
      );
      logD(
        'Start ${task.url} '
        'Request range：${task.startRange}-${task.endRange}',
      );

      Uint8List? data = await cache(task);
      // if the task has been added, wait for the download to complete
      bool exitUri = VideoProxy.downloadManager.isTaskExit(task);
      if (exitUri) {
        while (data == null) {
          await Future.delayed(const Duration(milliseconds: 100));
          data = await cache(task);
        }
      }
      if (data == null) {
        concurrent(task, headers);
        task.priority += 2;
        data = await download(task);
      }
      if (data == null) {
        retry--;
        if (retry == 0) {
          downloading = false;
          break;
        }
        continue;
      }

      int startIndex = 0;
      int? endIndex;
      if (startRange < requestRangeStart) {
        startIndex = requestRangeStart - startRange;
      }
      if (endRange > requestRangeEnd) {
        endIndex = requestRangeEnd - startRange + 1;
      }
      data = data.sublist(startIndex, endIndex);
      socket.done.then((value) {
        downloading = false;
      }).catchError((e) {
        downloading = false;
      });
      bool success = await socket.append(data);
      if (!success) downloading = false;
      startRange += Config.segmentSize;
      endRange = startRange + Config.segmentSize - 1;
      if (startRange > requestRangeEnd) {
        downloading = false;
      }
    }
  }

  /// Sends a HEAD request to get the content length of the resource at [uri].
  ///
  /// Returns the content length as an [int].
  Future<int> head(Uri uri, {Map<String, Object>? headers}) async {
    Dio client = VideoProxy.httpClientBuilderImpl.create();
    if (headers != null) {
      headers.forEach((key, value) {
        String keyLower = key.toLowerCase();
        if (keyLower == 'host' && value == Config.serverUrl) return;
        if (keyLower == 'range') return;
        client.options.headers[key] = value;
      });
    }
    try {
      final response = await client.headUri(uri);
      final length = _parseTotalLengthFromHeaders(response.headers);
      if (length > 0) return length;
    } catch (_) {}

    try {
      final probeHeaders = <String, dynamic>{...client.options.headers};
      probeHeaders[HttpHeaders.rangeHeader] = 'bytes=0-0';
      final response = await client.getUri(
        uri,
        options: Options(
          headers: probeHeaders,
          responseType: ResponseType.bytes,
          receiveTimeout: const Duration(seconds: 10),
          sendTimeout: const Duration(seconds: 10),
        ),
      );
      final length = _parseTotalLengthFromHeaders(response.headers);
      if (length > 0) return length;
    } finally {
      client.close(force: true);
    }

    return -1;
  }

  Future<void> _cacheContentLength(
    DownloadTask task,
    int contentLength,
  ) async {
    try {
      // Content length is already known by the caller. This small metadata file
      // only speeds up later requests, so failures must not block the response.
      String filePath = '${await FileExt.createCachePath(task.uri.generateMd5)}'
          '/${task.matchUrl}.meta';
      File file = File(filePath);
      await file.writeAsString(contentLength.toString());
      await LruCacheSingleton().storagePut(task.matchUrl, file);
    } catch (e) {
      logE('[UrlParserMp4] Cache content length failed: $e');
    }
  }

  /// Manages concurrent download tasks.
  ///
  /// Ensures that no more than 3 concurrent downloads are active for the same URL.
  /// If the number of concurrent downloads is less than 3, creates and adds new tasks.
  Future<void> concurrent(
    DownloadTask task,
    Map<String, String> headers,
  ) async {
    DownloadTask newTask = task;
    int activeSize = VideoProxy.downloadManager.allTasks
        .where((e) => e.url == newTask.url)
        .length;
    while (activeSize < 2) {
      newTask = DownloadTask(
        uri: newTask.uri,
        startRange: newTask.startRange + Config.segmentSize,
        endRange: newTask.startRange + Config.segmentSize * 2 - 1,
        headers: headers,
      );
      bool isExit = VideoProxy.downloadManager.allTasks
          .where((e) => e.matchUrl == newTask.matchUrl)
          .isNotEmpty;
      Uint8List? dataMemory = await LruCacheSingleton().memoryGet(
        newTask.matchUrl,
      );
      if (dataMemory != null) isExit = true;
      newTask.cacheDir = await FileExt.createCachePath(newTask.uri.generateMd5);
      File file = File(newTask.savePath);
      if (file.existsSync()) isExit = true;
      if (isExit) continue;
      logD("Asynchronous download start： ${newTask.toString()}");
      await VideoProxy.downloadManager.executeTask(newTask);
      activeSize = VideoProxy.downloadManager.allTasks
          .where((e) => e.url == newTask.url)
          .length;
    }
  }

  /// Whether the video is cached.
  ///
  /// [url]: The video URL to check.
  /// [headers]: Optional HTTP headers to use for the request.
  /// [cacheSegments]: Number of segments to cache.
  ///
  /// Returns `true` if the video is cached, otherwise `false`.
  @override
  Future<bool> isCached(
    String url,
    Map<String, Object>? headers,
    int cacheSegments,
  ) async {
    // 🚀 极速优化：先检测第 0 分片（开头分片）是否缓存在本地。
    // 如果第 0 分片不在，那说明该视频完全未被缓存，直接返回 false！
    // 这样可以 100% 避免后面由于 contentLength 未知而触发的超慢 HEAD 联网请求，秒级返回！
    try {
      DownloadTask firstTask = DownloadTask(uri: url.toSafeUri(), headers: headers);
      firstTask.startRange = 0;
      firstTask.endRange = Config.segmentSize - 1;
      Uint8List? firstData = await cache(firstTask);
      if (firstData == null) {
        return false;
      }
    } catch (_) {}

    int contentLength = 0;
    
    // 1. 尝试从本地缓存中直接读取 content_length 避免网络请求
    try {
      final infoTask = _contentLengthTask(
        url.toSafeUri(),
        (headers ?? const <String, Object>{})
            .map((k, v) => MapEntry(k, v.toString())),
      );
      final Uint8List? infoData = await cache(infoTask);
      if (infoData != null) {
        contentLength = int.tryParse(Utf8Codec().decode(infoData)) ?? 0;
      }
    } catch (_) {}

    // 2. 尝试从本地目录寻找 .meta 文件获取 content_length
    if (contentLength <= 0) {
      try {
        contentLength = await _findContentLengthLocally(url.toSafeUri());
      } catch (_) {}
    }

    // 3. 如果本地没有缓存好的元数据长度，再尝试联网 HEAD（降级容灾）
    if (contentLength <= 0) {
      try {
        contentLength = await head(url.toSafeUri(), headers: headers);
      } catch (_) {}
    }

    if (contentLength > 0) {
      int segmentSize = contentLength ~/ Config.segmentSize +
          (contentLength % Config.segmentSize > 0 ? 1 : 0);
      if (cacheSegments > segmentSize) {
        cacheSegments = segmentSize;
      }
    } else {
      // 3. 如果完全断网（contentLength 依然 <= 0），检查本地分片目录是否存在。
      // 如果本地目录存在且里面包含已经下载的分片，判定为已缓存，防止断网抛异常！
      try {
        final cacheKey = url.toSafeUri().generateMd5;
        final cachePath = await FileExt.createCachePath(cacheKey);
        final dir = Directory(cachePath);
        if (await dir.exists()) {
          final files = dir.listSync();
          if (files.isNotEmpty) {
            // 只要里面存在缓存分片，说明已经缓冲过，返回 true
            return true;
          }
        }
      } catch (_) {}
      return false;
    }

    int count = 0;
    while (count < cacheSegments) {
      DownloadTask task = DownloadTask(uri: url.toSafeUri(), headers: headers);
      // Set the start and end range for each segment
      task.startRange += Config.segmentSize * count;
      task.endRange = task.startRange + Config.segmentSize - 1;
      count++;
      Uint8List? data = await cache(task);
      if (data == null) return false;
    }
    return true;
  }

  /// Pre-caches data from the network.
  ///
  /// [cacheSegments]: Number of segments to cache.<br>
  /// [downloadNow]: If true, downloads immediately; otherwise, pushes tasks to the queue.<br>
  /// [progressListen]: If true, returns a [StreamController] with progress updates.
  ///
  /// Returns a [StreamController] emitting progress maps, or `null` if not listening.
  @override
  Future<StreamController<Map>?> precache(
    String url,
    Map<String, Object>? headers,
    int cacheSegments,
    bool downloadNow,
    bool progressListen,
  ) async {
    StreamController<Map>? _streamController;
    if (progressListen) _streamController = StreamController();
    int contentLength = await head(url.toSafeUri(), headers: headers);
    if (contentLength > 0) {
      final infoTask = _contentLengthTask(
        url.toSafeUri(),
        (headers ?? const <String, Object>{})
            .map((k, v) => MapEntry(k, v.toString())),
      );
      await _cacheContentLength(infoTask, contentLength);

      int segmentSize = contentLength ~/ Config.segmentSize +
          (contentLength % Config.segmentSize > 0 ? 1 : 0);
      if (cacheSegments > segmentSize) {
        cacheSegments = segmentSize;
      }
    }
    int downloadedSize = 0;
    int totalSize = cacheSegments;
    int count = 0;
    while (count < cacheSegments) {
      DownloadTask task = DownloadTask(uri: url.toSafeUri(), headers: headers);
      task.startRange += Config.segmentSize * count;
      task.endRange = task.startRange + Config.segmentSize - 1;
      count++;
      if (downloadNow) {
        Uint8List? data = await cache(task);
        if (data != null) {
          downloadedSize += 1;
          _streamController?.sink.add({
            'progress': downloadedSize / totalSize,
            'url': task.url,
            'startRange': task.startRange,
            'endRange': task.endRange,
          });
          continue;
        }
        download(task).whenComplete(() {
          downloadedSize += 1;
          _streamController?.sink.add({
            'progress': downloadedSize / totalSize,
            'url': task.url,
            'startRange': task.startRange,
            'endRange': task.endRange,
          });
        });
      } else {
        push(task);
      }
    }
    return _streamController;
  }

  /// Pushes the [task] to the download manager for processing.
  /// If the task is already in the download manager or cache, does nothing.
  @override
  Future<void> push(DownloadTask task) async {
    Uint8List? dataMemory = await LruCacheSingleton().memoryGet(task.matchUrl);
    if (dataMemory != null) return;
    String cachePath = await FileExt.createCachePath(task.uri.generateMd5);
    File file = File('$cachePath/${task.saveFileName}');
    if (await file.exists()) return;
    task.cacheDir = cachePath;
    await VideoProxy.downloadManager.addTask(task);
  }
}
