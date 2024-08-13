unit aardio;

interface

uses
  Windows;

function sendCallback(hwnd: HWND; structProto: String; struct: Pointer): Integer;

implementation

function sendCallback(hwnd: HWND; structProto: String; struct: Pointer): Integer;
begin
  Result := Windows.SendMessage(hwnd,$ACCE{ _WM_THREAD_CALLBACK },integer(PChar(structProto)),integer(struct));
end;

end.
