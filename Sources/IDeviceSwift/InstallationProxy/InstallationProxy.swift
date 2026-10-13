//
//  ConduitInstaller.swift
//  Feather
//
//  Created by samara on 23.04.2025.
//

import Foundation
import SwiftUI
import IDevice
import UIKit.UIApplication

public class InstallationProxy: Identifiable, ObservableObject {
	private let _heartbeat = HeartbeatManager.shared
	private let _uuid = UUID().uuidString
	
	typealias AfcClientHandle = OpaquePointer
	typealias AfcFileHandle = OpaquePointer
	typealias InstallationProxyClientHandle = OpaquePointer
	
	@ObservedObject
	public var viewModel: InstallerStatusViewModel
	
	private let _diagnosticLog: @Sendable (String) -> Void

	public init(viewModel: InstallerStatusViewModel, diagnosticLog: @escaping @Sendable (String) -> Void = { NSLog("%@", $0) }) {
		self.viewModel = viewModel
		self._diagnosticLog = diagnosticLog
	}

	private func _log(_ message: String) {
		_diagnosticLog("attempt=\(_uuid) \(message)")
	}
	
	public func install(at url: URL, suspend: Bool = false) async throws {
		var afcClient: AfcClientHandle?
		var fileHandle: AfcFileHandle?
		var installproxy: InstallationProxyClientHandle?
		
		try await Task.detached(priority: .utility) {
			let attemptStarted = ProcessInfo.processInfo.systemUptime
			var phase = "connection"
			self._log("diagnostics=5 transport=jktcp-0.1.7-nodelay-tlsfix phase=connection started")
			defer {
				self._log("attempt ended phase=\(phase) elapsed=\(ProcessInfo.processInfo.systemUptime - attemptStarted)s")
			}
			guard FileManager.default.fileExists(atPath: HeartbeatManager.pairingFile()) else {
				throw IDeviceSwiftError(message: "Missing Pairing")
			}
			
			if await self._heartbeat.isRsd {
				guard await self._heartbeat.ensureRSDTunnel() else {
					throw IDeviceSwiftError(message: "Missing Pairing")
				}
			} else {
				guard await self._heartbeat.checkSocketConnection().isConnected else {
					throw IDeviceSwiftError(message: "Missing Pairing")
				}
			}
			
			defer {
				afc_client_free(afcClient)
				installation_proxy_client_free(installproxy)
			}
			
			let heartbeat = await self._heartbeat
			
			if heartbeat.isRsd {
				guard let adapter = heartbeat.adapter else {
					throw IDeviceSwiftError(message: "Cannot find RSD adapter")
				}
				
				guard let handshake = heartbeat.handshake else {
					throw IDeviceSwiftError(message: "Cannot find RSD handshake")
				}
	
				guard afc_client_connect_rsd(adapter, handshake, &afcClient) == nil else {
					throw IDeviceSwiftError(message: "Cannot connect to AFC")
				}
			} else {
				guard let provider = heartbeat.provider else {
					throw IDeviceSwiftError(message: "Cannot find TCP provider")
				}
				
				guard afc_client_connect(provider, &afcClient) == nil else {
					throw IDeviceSwiftError(message: "Cannot connect to AFC")
				}
			}
			
			let stagingDir = "PublicStaging"
			
			let afc_make_directory_result =  afc_make_directory(afcClient, stagingDir)
			guard afc_make_directory_result == nil else {
				throw IDeviceSwiftError(afc_make_directory_result)
			}
			
			print("1")
			
			let remoteDir = "/\(stagingDir)/\(self._uuid).ipa"
			
			let afc_file_open_result = afc_file_open(afcClient, remoteDir, AfcWrOnly, &fileHandle)
			guard afc_file_open_result == nil else {
				throw IDeviceSwiftError(afc_file_open_result)
			}
			
			print("2")
			
			phase = "payload"
			self._log("phase=payload started")
			try await self._updateStatus(with: .sendingPayload)
			
			guard let fileHandle = fileHandle else {
				throw IDeviceSwiftError(message: "Missing File Handle")
			}
			
			do {
				let input = try FileHandle(forReadingFrom: url)
				defer { try? input.close() }
				let totalSize = try input.seekToEnd()
				try input.seek(toOffset: 0)
				guard totalSize > 0 else {
					throw IDeviceSwiftError(message: "IPA is empty")
				}
				// The sliding-window transport drains a queue without cloning its remainder.
				// Match AFC's maximum write size to avoid extra service round trips.
				let chunkSize = 1024 * 1024
				self._log("payload writeBytes=\(chunkSize) totalBytes=\(totalSize)")
				var totalBytesWritten: UInt64 = 0
				let started = ProcessInfo.processInfo.systemUptime
				var readSeconds = 0.0
				var writeSeconds = 0.0
				var lastLog = started
				var lastProgress = started
				defer {
					let elapsed = ProcessInfo.processInfo.systemUptime - started
					self._log(String(format: "KorSign payload: bytes=%llu/%llu elapsed=%.3fs read=%.3fs AFC=%.3fs rate=%.2f MiB/s", totalBytesWritten, totalSize, elapsed, readSeconds, writeSeconds, Double(totalBytesWritten) / 1048576 / max(elapsed, 0.001)))
				}
				while totalBytesWritten < totalSize {
					try Task.checkCancellation()
					let readStart = ProcessInfo.processInfo.systemUptime
					guard let chunk = try input.read(upToCount: Int(min(UInt64(chunkSize), totalSize - totalBytesWritten))), !chunk.isEmpty else {
						throw IDeviceSwiftError(message: "IPA ended before upload completed")
					}
					readSeconds += ProcessInfo.processInfo.systemUptime - readStart
					let writeStart = ProcessInfo.processInfo.systemUptime
					// The pointer must remain inside withUnsafeBytes for the entire FFI call.
					let result = chunk.withUnsafeBytes { buffer in
						afc_file_write(fileHandle, buffer.bindMemory(to: UInt8.self).baseAddress!, chunk.count)
					}
					writeSeconds += ProcessInfo.processInfo.systemUptime - writeStart
					if result != nil {
						throw IDeviceSwiftError(result)
					}
					totalBytesWritten += UInt64(chunk.count)
					let now = ProcessInfo.processInfo.systemUptime
					if now - lastProgress >= 0.1 || totalBytesWritten == totalSize {
						try await self._updateUploadProgress(with: Double(totalBytesWritten) / Double(totalSize))
						lastProgress = now
					}
					if now - lastLog >= 5 {
						self._log(String(format: "KorSign payload progress: bytes=%llu/%llu read=%.3fs AFC=%.3fs", totalBytesWritten, totalSize, readSeconds, writeSeconds))
						lastLog = now
					}
				}
			}

			print(2)
			
			let afc_file_close_result = afc_file_close(fileHandle)
			guard afc_file_close_result == nil else {
				throw IDeviceSwiftError(afc_file_close_result)
			}
			
			phase = "installing"
			self._log("phase=installing started")
			try await self._updateStatus(with: .installing)
			
			
			if heartbeat.isRsd {
				guard let adapter = heartbeat.adapter else {
					throw IDeviceSwiftError(message: "Cannot find RSD adapter")
				}
				
				guard let handshake = heartbeat.handshake else {
					throw IDeviceSwiftError(message: "Cannot find RSD handshake")
				}
				
				print(3)
				
				let installation_proxy_connect_rsd_result = installation_proxy_connect_rsd(adapter, handshake, &installproxy)
				guard installation_proxy_connect_rsd_result == nil else {
					throw IDeviceSwiftError(installation_proxy_connect_rsd_result)
				}
			} else {
				guard let provider = heartbeat.provider else {
					throw IDeviceSwiftError(message: "Cannot find TCP provider")
				}
				let installation_proxy_connect_result = installation_proxy_connect(provider, &installproxy)
				guard installation_proxy_connect_result == nil else {
					throw IDeviceSwiftError(installation_proxy_connect_result)
				}
			}
			
			print(4)
			
			let installError: UnsafeMutablePointer<IdeviceFfiError>? = remoteDir.withCString { cString in
				let context = Unmanaged.passUnretained(self).toOpaque()
				
				if suspend {
					DispatchQueue.main.async {
						UIApplication.shared.perform(#selector(NSXPCConnection.suspend))
					}
				}
				
				return installation_proxy_install_with_callback(
					installproxy,
					cString,
					nil, // options
					Self._installationProgressCallback,
					context
				)
			}
			
			guard installError == nil else {
				throw IDeviceSwiftError(installError)
			}
			
			try await Task.sleep(nanoseconds: 350_000_000)
			phase = "completed"
			self._log("phase=completed")
			try await self._updateStatus(with: .completed(.success(())))
		}.value
	}
	
	private func _updateStatus(with status: InstallerStatusViewModel.InstallerStatus) async throws {
		await MainActor.run {
			self.viewModel.status = status
		}
	}
	
	private func _updateUploadProgress(with status: Double) async throws {
		await MainActor.run {
			self.viewModel.uploadProgress = status
		}
	}
	
	nonisolated static private let _installationProgressCallback: @convention(c) (
		UInt64,
		UnsafeMutableRawPointer?
	) -> Void = { progress, context in
		guard let context = context else { return }
		let installer = Unmanaged<InstallationProxy>.fromOpaque(context).takeUnretainedValue()
		Task {
			try? await installer._updateInstallProgress(with: Double(progress) / 100.0)
		}
	}
	
	private func _updateInstallProgress(with status: Double) async throws {
		await MainActor.run {
			self.viewModel.installProgress = status
		}
	}
}
