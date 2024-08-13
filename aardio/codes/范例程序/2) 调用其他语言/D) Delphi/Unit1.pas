unit Unit1;

interface

uses
  Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms,
  Dialogs, StdCtrls, aardio;

function CreateForm(parentHandle: HWND): HWND; stdcall;

type
  TForm1 = class(TForm)
    Button1: TButton;
    procedure Button1Click(Sender: TObject);
    procedure FormDestroy(Sender: TObject); 
  public
    { Public declarations }
  end;

type
    TStruct = record
    x:integer;
    y:integer;
  end;

var TestStruct:TStruct;

implementation


function CreateForm(parentHandle: HWND): HWND; stdcall;
var
  Form1 : TForm1;
begin
  TestStruct.x := 12;
  TestStruct.y := 34;

  Form1 := TForm1.CreateParented(parentHandle);
  Form1.Show;
  Result := Form1.Handle;
end;

{$R *.dfm}

procedure TForm1.Button1Click(Sender: TObject);

begin
  aardio.sendCallback(self.ParentWindow,'onTest( { int x;int y;} )',@TestStruct);
end;

procedure TForm1.FormDestroy(Sender: TObject);
begin
  self.Free; 
end;

end.
