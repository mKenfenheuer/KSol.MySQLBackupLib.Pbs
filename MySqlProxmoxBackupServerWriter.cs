using KSol.MySQLBackupLib;
using PbsClientDotNet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KSol.MySqlBackupConsole
{
    public class MySqlProxmoxBackupServerWriter : IMySqlBackupWriter
    {
        int _wid = -1;
        BackupApiClient _api;
        public MySqlProxmoxBackupServerWriter(string url, string fingerprint)
        {
            _api = new BackupApiClient(
                        url: url,
                        fingerprint: fingerprint);
            Console.WriteLine("Login Successful.");
        }

        public async Task Login(string username, string password)
        {
            var response = await _api.LoginAsync(
                    username: username,
                    password: password);
            if (!response.IsSuccess)
                throw new Exception($"Could not login: {response.ErrorMessage}");
        }

        public async Task StartHostBackup(string hostname, string datastore)
        {
            await _api.StartBackupProtocol($"mysql-{hostname}", "host", datastore, DateTime.UtcNow);
        }

        public async Task EndHostBackup()
        {
            await _api.FinishBackupProtocol();
        }

        public async Task CancelBackup()
        {
            if (_wid != -1)
            {
                await _api.AppendChunksToDynamicIndex(_wid);
                await _api.CloseDynamicIndex(_wid);
                _wid = -1;
            }
        }

        public async Task EndBackup()
        {
            await _api.AppendChunksToDynamicIndex(_wid);
            await _api.CloseDynamicIndex(_wid);
            _wid = -1;
        }

        public async Task StartBackup(string filename)
        {
            if (_wid != -1)
                throw new Exception("Cannot start a backup with another backup in progress!");

            _wid = await _api.CreateDynamicIndex(filename);
        }

        public async Task WriteBackupChunk(byte[] chunk)
        {
            await _api.UploadDynamicChunk(_wid, chunk);
        }

        public async Task CommitBackupChunks()
        {
            await _api.AppendChunksToDynamicIndex(_wid);
        }
    }
}
